import requests
import sqlite3
import csv
import sys
from datetime import datetime

db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# NEW SCHEMA: Each row is a specific "Event" in time
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticker_event_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cik INTEGER,
        ticker TEXT,
        name TEXT,
        event_scenario TEXT, -- 'NEW_LISTING', 'TICKER_CHANGE', 'DELISTED', 'ACTIVE_SYNC'
        is_active INTEGER,
        timestamp DATETIME
    )
''')

headers = {'User-Agent': 'RyanSchraub (ryan.schraub@gmail.com)'}
url = "https://www.sec.gov/files/company_tickers.json"

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    # 1. Check for New Listings or Ticker Changes
    for cik, info in incoming_ciks.items():
        ticker = info['ticker']
        name = info['title']

        # Get the LATEST known state for this CIK
        cursor.execute('''
            SELECT ticker, name, is_active FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        if not latest:
            scenario = "NEW_LISTING"
        elif latest[0] != ticker:
            scenario = "TICKER_CHANGE"
        elif latest[2] == 0: # If it was previously delisted but came back
            scenario = "RE_LISTED"
        
        # If a change happened, log a new row
        if scenario:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, scenario, current_time))

    # 2. Check for Delistings (Scenario: DELISTED)
    # Find CIKs that are currently marked 'active' in our DB but are NOT in the SEC file
    cursor.execute('''
        SELECT DISTINCT cik, ticker, name FROM ticker_event_log t1
        WHERE is_active = 1 
        AND timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
    ''')
    current_actives = cursor.fetchall()

    for cik, ticker, name in current_actives:
        if cik not in incoming_ciks:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, 'DELISTED', 0, ?)
            ''', (cik, ticker, name, current_time))

    conn.commit()

    # 3. Export "Current State" for the CSV Preview
    # This complex query only grabs the MOST RECENT row for every company
    cursor.execute('''
        SELECT ticker, cik, name, event_scenario, is_active, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        ORDER BY ticker ASC
    ''')
    
    with open('tickers_preview.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Last Event', 'Active', 'Date'])
        writer.writerows(cursor.fetchall())

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
finally:
    conn.close()
