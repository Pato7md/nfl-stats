import pandas as pd
from sqlalchemy import create_engine

# Excel-Datei-Pfad
excel_file = 'C:/Users/Michael Dürrschmidt/OneDrive - etepetete/Desktop/Nfl Season 2024.xlsm'

# PostgreSQL-Verbindungsdaten anpassen
db_user = 'postgres'
db_password = 'Mic$Tam7373'
db_host = 'localhost'
db_port = '5432'
db_name = 'postgres'

# Verbindung zu PostgreSQL aufbauen
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Dictionary: Sheet-Name → Ziel-Tabellenname
sheets_to_tables = {
    'Kicker': 'kicker',
    'QB Stats': 'qb_stats',
    'RB Stats': 'rb_stats',
    'WR Stats': 'wr_stats',
    'QB Advanced': 'qb_advanced',
    'RB Advanced': 'rb_advanced',
    'WR Advanced': 'wr_advanced',
    'Defense': 'defense'
}


# Sheets durchgehen und einzeln importieren
for sheet_name, table_name in sheets_to_tables.items():
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f'✅ Sheet "{sheet_name}" erfolgreich in Tabelle "{table_name}" importiert.')
