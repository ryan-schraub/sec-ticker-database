import requests
import sqlite3
import csv
import sys
from datetime import datetime

# CONFIGURATION
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com"
SEC_URL = "https://www.sec.gov/files/company_tickers.json"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Ensure schema exists
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

    # 2. Check if this is the very first run
    cursor.execute("SELECT COUNT(*) FROM ticker_event_log")
    is_first_run = cursor.fetchone()[0] == 0
    
    # 3. Fetch data from SEC
    headers = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
    response = requests.get(SEC_URL, headers=headers)
    data = response.json()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    # 4. Process all companies
    for cik, info in incoming_ciks.items():
        ticker = str(info['ticker']).upper()
        name = info['title']

        # Get the most recent record for this CIK
        cursor.execute('''
            SELECT ticker, name FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        
        if not latest:
            # FIX: If first run, don't say NEW_LISTING
            scenario = "-" if is_first_run else "NEW_LISTING"
        elif latest[0] != ticker:
            # Specific logic for AAM / Dauch Rebranding
            if ticker == "DCH" and "DAUCH" in name.upper():
                scenario = f"REBRAND: AAM rebranded to Dauch Corp ({latest[0]} → {ticker})"
            else:
                scenario = f"TICKER_CHANGE: {latest[0]} → {ticker}"
        elif latest[1] != name:
            scenario = f"NAME_CHANGE: {latest[1]} → {name}"
        
        if scenario:
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, event_scenario, is_active, timestamp)
                VALUES (?, ?, ?, ?, 1, ?)
            ''', (cik, ticker, name, scenario, current_time))

    conn.commit()

    # 5. Export for Website
    cursor.execute('''
        SELECT ticker, cik, name, event_scenario, timestamp 
        FROM ticker_event_log t1
        WHERE timestamp = (SELECT MAX(timestamp) FROM ticker_event_log t2 WHERE t2.cik = t1.cik)
        AND is_active = 1
        ORDER BY ticker ASC
    ''')
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Name', 'Last Event', 'Date Detected'])
        writer.writerows(cursor.fetchall())

    conn.close()

if __name__ == "__main__":
    main()
