import requests
import sqlite3
import csv
import time
from datetime import datetime

# --- CONFIG ---
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. DATABASE SETUP & AUTOMATIC MIGRATION
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT,
            event_scenario TEXT, 
            next_earnings TEXT, 
            last_filing TEXT,
            filing_url TEXT,
            timestamp DATETIME
        )
    ''')
    
    # Check if new columns exist, if not, add them (Fixes your screenshot error)
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    cols = [info[1] for info in cursor.fetchall()]
    if 'last_filing' not in cols: cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN last_filing TEXT')
    if 'filing_url' not in cols: cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN filing_url TEXT')

    # 2. GET MASTER LIST
    print("Fetching master ticker list...")
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    
    # We will process the first 150 tickers to stay within GitHub Action time limits
    # You can adjust this range or remove the slice for a full database update
    tickers_to_process = list(master_data.values())[:150] 
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in tickers_to_process:
        cik = str(item['cik_str']).zfill(10)
        ticker = item['ticker'].upper()
        name = item['title']
        
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, last_filing, filing_url, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (int(cik), ticker, name, "-", "2026-05-15 (Legal Fallback)", "None Found", "", now))

        # 3. FETCH HISTORICAL DATA (Gets filings from 2 days ago and beyond)
        try:
            time.sleep(0.12) # Stay under 10 requests/sec SEC limit
            sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            response = requests.get(sub_url, headers=HEADERS)
            
            if response.status_code == 200:
                data = response.json()
                recent = data.get('filings', {}).get('recent', {})
                
                found = False
                for i, form in enumerate(recent.get('form', [])):
                    if form in ['10-K', '10-Q']:
                        acc = recent['accessionNumber'][i].replace('-', '')
                        doc = recent['primaryDocument'][i]
                        date = recent['reportDate'][i]
                        
                        link = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}"
                        
                        cursor.execute('''
                            UPDATE ticker_event_log 
                            SET last_filing = ?, filing_url = ?, timestamp = ?
                            WHERE ticker = ?
                        ''', (f"{form} ({date})", link, now, ticker))
                        found = True
                        break 
        except Exception as e:
            print(f"Skipping {ticker}: {e}")

    conn.commit()

    # 4. EXPORT
    cursor.execute("SELECT ticker, name, next_earnings, last_filing, filing_url FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Earnings', 'Last 10-K/Q', 'Filing Link'])
        writer.writerows(rows)
    conn.close()

if __name__ == "__main__":
    main()
