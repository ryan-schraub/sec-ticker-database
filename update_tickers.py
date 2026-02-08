import requests
import sqlite3
import csv
import sys
from datetime import datetime

# 1. Setup Database with History Support
db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Main Table: Current source of truth
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sec_tickers (
        cik INTEGER PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        is_active INTEGER DEFAULT 1,
        last_updated DATETIME
    )
''')

# History Table: Tracks every time a ticker changes
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticker_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cik INTEGER,
        old_ticker TEXT,
        new_ticker TEXT,
        change_date DATETIME
    )
''')

# 2. Fetch Data
headers = {'User-Agent': 'RyanSchraub (ryan.schraub@gmail.com)'}
url = "https://www.sec.gov/files/company_tickers.json"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = set()

    # 3. Process Updates & History
    for entry in data.values():
        cik = entry['cik_str']
        ticker = entry['ticker']
        name = entry['title']
        incoming_ciks.add(cik)

        # Check for ticker changes before updating
        cursor.execute("SELECT ticker FROM sec_tickers WHERE cik = ?", (cik,))
        existing = cursor.fetchone()

        if existing and existing[0] != ticker:
            # Ticker changed! Log it in history
            cursor.execute('''
                INSERT INTO ticker_history (cik, old_ticker, new_ticker, change_date)
                VALUES (?, ?, ?, ?)
            ''', (cik, existing[0], ticker, current_time))

        # Upsert the main record
        cursor.execute('''
            INSERT INTO sec_tickers (cik, ticker, name, is_active, last_updated)
            VALUES (?, ?, ?, 1, ?)
            ON CONFLICT(cik) DO UPDATE SET
                ticker=excluded.ticker,
                name=excluded.name,
                is_active=1,
                last_updated=excluded.last_updated
        ''', (cik, ticker, name, current_time))

    # 4. Handle Delistings (Mark inactive if missing from SEC file)
    cursor.execute("UPDATE sec_tickers SET is_active = 0 WHERE cik NOT IN ({})".format(
        ','.join(['?'] * len(incoming_ciks))), list(incoming_ciks))

    conn.commit()

    # 5. Export Daily CSV Preview
    cursor.execute("SELECT ticker, cik, name, is_active FROM sec_tickers ORDER BY ticker ASC")
    with open('tickers_preview.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Active Status'])
        writer.writerows(cursor.fetchall())

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
finally:
    conn.close()
