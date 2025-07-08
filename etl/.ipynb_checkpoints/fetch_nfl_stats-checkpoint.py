import sys
import requests
import pandas as pd
import re
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime

# PostgreSQL-Verbindungsdaten anpassen
db_user = 'postgres'
db_password = 'Mic$Tam7373'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'

# SQLAlchemy-Engine
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def fetch_nfl_stats(position: str, week: int = None, year: int = None):
    base_urls = {
        'qb':      'https://www.fantasypros.com/nfl/stats/qb.php',
        'rb':      'https://www.fantasypros.com/nfl/stats/rb.php',
        'wr':      'https://www.fantasypros.com/nfl/stats/wr.php',
        'te':      'https://www.fantasypros.com/nfl/stats/te.php',
        'k':       'https://www.fantasypros.com/nfl/stats/k.php',
        'dl':      'https://www.fantasypros.com/nfl/stats/dl.php', 
        'lb':      'https://www.fantasypros.com/nfl/stats/lb.php', 
        'db':      'https://www.fantasypros.com/nfl/stats/db.php', 
        'qb_adv':  'https://www.fantasypros.com/nfl/advanced-stats-qb.php',
        'rb_adv':  'https://www.fantasypros.com/nfl/advanced-stats-rb.php',
        'wr_adv':  'https://www.fantasypros.com/nfl/advanced-stats-wr.php',
        'te_adv':  'https://www.fantasypros.com/nfl/advanced-stats-te.php'
    }

    position = position.lower()
    if position not in base_urls:
        print(f"Ungültige Position '{position}'. Verfügbare: {list(base_urls.keys())}")
        sys.exit(1)

    url = base_urls[position]

    # Query-Parameter aufbauen
    params = {}
    if week is not None:
        params['week'] = str(week)
        params['range'] = 'week'
    if year is not None:
        params['year'] = str(year)

    # Nur wenn params nicht leer: an URL anhängen
    if params:
        from urllib.parse import urlencode
        url += '?' + urlencode(params)

    print(f"Lade Daten von: {url}")

    response = requests.get(url)
    response.raise_for_status()

    dfs = pd.read_html(StringIO(response.text))
    df = dfs[0]

    # Spalten bereinigen (MultiIndex zu flach)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [' '.join(col).strip() for col in df.columns.values]
    else:
        df.columns = [col.strip() for col in df.columns]

    # Neue Spalten hinzufügen
    df['week'] = week
    df['year'] = year
    df['position'] = position.upper() 
    df['imported_at'] = datetime.now()

    # Spalten zu löschen, nicht benötigte
    columns_to_drop = [
        'G',
        'MISC G',
        'MISC FPTS/G',
        'FPTS/G',
        'MISC FL'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')


    # Spalten umbenennen
    df = df.rename(columns={
        'Unnamed: 0_level_0 Rank': 'rank',
        'Rank': 'rank',
        'Unnamed: 1_level_0 Player': 'player',
        'Player': 'player',
        'PASSING CMP': 'pass_cmp',
        'PASSING ATT': 'pass_att',
        'PASSING PCT': 'pass_pct',
        'PASSING YDS': 'pass_yds',
        'PASSING Y/A': 'pass_y/a',
        'PASSING TD':  'pass_td',
        'PASSING INT': 'pass_int',
        'PASSING SACKS': 'pass_sacks',
        'RUSHING ATT': 'rush_att',
        'RUSHING YDS': 'rush_yds',
        'RUSHING Y/A': 'rush y/a',
        'RUSHING LG': 'rush_lg',
        'RUSHING 20+': 'rush_20+',
        'RUSHING TD': 'rush_td',
        'RECEIVING REC': 'recv_rec', 
        'RECEIVING TGT': 'recv_tgt',
        'RECEIVING YDS': 'recv_yds',
        'RECEIVING Y/R': 'recv_y/r',
        'RECEIVING TD': 'recv_td',
        'RECEIVING LG': 'recv_lg',
        'RECEIVING 20+': 'recv_20+',
        'MISC FPTS': 'fpts',
        'MISC ROST': 'rost',
        'FG': 'k_fgm',
        'FGA': 'k_fga',
        'PCT': 'k_pct',
        'LG': 'k_lg',
        '1-19': 'k_1-19',
        '20-29': 'k_20-29',
        '30-39': 'k_30-39',
        '40-49': 'k_40-49',
        '50+': 'k_50+',
        'XPT': 'k_xpm',
        'XPA': 'k_xpa',
        'FPTS': 'fpts',
        'ROST': 'rost',
        'TACKLE': 'def_tackle',
        'ASSIST': 'def_assist',
        'SACK': 'def_sack',
        'PD': 'def_pd',
        'INT': 'def_int',
        'FF': 'def_ff',
        'FR': 'def_fr',
        'DEF TD': 'def_td'
    })

    # Splitte Spalte Player in Player und Team
    df['team'] = df['player'].str.extract(r'\(([^)]+)\)$')
    df['player'] = df['player'].str.replace(r'\s*\([^)]+\)$', '', regex=True)
    player_index = df.columns.get_loc("player")
    team_column = df.pop("team")
    df.insert(player_index + 1, "team", team_column)


    # Zusammenfassung Receiving und Defense Tabellen
    if position in ['wr', 'te']:
        table_name = 'rec_stats'
    elif position in ['dl', 'lb', 'db']:
        table_name = 'def_stats'
    else:
        table_name = f"{position}_stats"


    # Überführung in SQL
    df.to_sql(table_name, engine, if_exists='append', index=False)

    print(f"Daten für Position '{position.upper()}' aus Woche {week}/{year} erfolgreich importiert.")


if __name__ == "__main__":
    # Falls keine Argumente übergeben wurden, mache kompletten Lauf
    if len(sys.argv) == 1:
        positions = ['qb', 'rb', 'wr', 'te', 'k', 'dl', 'lb', 'db']
#        positions = ['qb']
        weeks = range(1, 19)
        for pos in positions:
            for week in weeks:
                print(f"Starte Import für Position {pos.upper()} Woche {week}")
                fetch_nfl_stats(pos, week, 2024)
    else:
        # Bestehende Logik mit Kommandozeilenargumenten
        argc = len(sys.argv)
        if argc < 2 or argc > 4:
            print("Usage: python fetch_nfl_stats.py position [week] [year]")
            sys.exit(1)

        pos = sys.argv[1]
        wk = int(sys.argv[2]) if argc >= 3 else None
        yr = int(sys.argv[3]) if argc == 4 else None

        fetch_nfl_stats(pos, wk, yr)