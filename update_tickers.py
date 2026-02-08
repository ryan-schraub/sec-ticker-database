import requests
import sqlite3
import csv
import sys
from datetime import datetime

# CONFIGURATION
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com" # Required for SEC Fair Access Policy
SEC_URL = "https://www.sec.gov/files/company_tickers.json"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. Create table with a column for 'event_scenario'
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

    # 2. Check if this is the very first time running (baseline)
    cursor.execute("SELECT COUNT(*) FROM ticker_event_log")
    is_database_empty = cursor.fetchone()[0] == 0
    
    # 3. Fetch data from SEC
    headers = {'User-Agent': f'SecurityMasterBot ({USER_EMAIL})'}
    try:
        response = requests.get(SEC_URL, headers=headers)
        data = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    incoming_ciks = {item['cik_str']: item for item in data.values()}

    # 4. Compare SEC data to your Database
    for cik, info in incoming_ciks.items():
        ticker = str(info['ticker']).upper()
        name = info['title']

        # Get the latest known state for this CIK
        cursor.execute('''
            SELECT ticker, name FROM ticker_event_log 
            WHERE cik = ? ORDER BY timestamp DESC LIMIT 1
        ''', (cik,))
        latest = cursor.fetchone()

        scenario = None
        
        if not latest:
            # FIX: If database was empty, it's just setup data (-). 
            # If database HAD data, this truly is a NEW_LISTING.
            scenario = "-" if is_database_empty else "NEW_LISTING"
        elif latest[0] != ticker:
            # Specific logic for the AAM rebranding to Dauch
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

    # 5. Export CURRENT ACTIVE tickers for the website
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
    print("Update complete.")

if __name__ == "__main__":
    main()
