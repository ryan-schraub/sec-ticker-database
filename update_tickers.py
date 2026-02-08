import requests
import sqlite3
import csv
import sys
from datetime import datetime

db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# 1. Database Setup
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticker_event_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cik INTEGER,
        ticker TEXT,
        name TEXT,
        event_scenario TEXT, 
        is_active INTEGER,
        timestamp DATETIME
    )
''')

# Check if this is the first time the script has EVER run
cursor.execute("SELECT COUNT(*) FROM ticker_event_log")
is_first_run = cursor.fetchone()[0] == 0

headers = {'User-Agent': 'RyanSchraub (ryan.schraub@gmail.com)'}
url = "https://www.sec.gov/files/company_tickers.json"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    # 2. Process Changes
    for cik, info in incoming_ciks.items():
        ticker = info['ticker']
        name = info['title']

        cursor.execute('''
            SELECT ticker, name, is_active FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        
        if not latest:
            # If the DB was empty, it's just seed data. If not, it's a real new listing.
            scenario = "SEED_DATA" if is_first_run else "NEW_LISTING"
        elif latest[0] != ticker:
            scenario = "TICKER_CHANGE"
        elif latest[2] == 0:
            scenario = "RE_LISTED"
        
        if scenario:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, scenario, current_time))

    # 3. Handle Delistings
    cursor.execute('''
        SELECT DISTINCT cik, ticker, name FROM ticker_event_log t1
        WHERE is_active = 1 
        AND timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
    ''')
    for cik, ticker, name in cursor.fetchall():
        if cik not in incoming_ciks:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, 'DELISTED', 0, ?)
            ''', (cik, ticker, name, current_time))

    conn.commit()

    # 4. Export Current State
    cursor.execute('''
        SELECT ticker, cik, name, event_scenario, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        AND is_active = 1
        ORDER BY ticker ASC
    ''')
    
    with open('tickers_preview.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Last Event', 'Date Detected'])
        writer.writerows(cursor.fetchall())

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
finally:
    conn.close()
