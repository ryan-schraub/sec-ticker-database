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

    # 1. DATABASE SETUP & MIGRATION
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
    
    # Ensure columns exist for older DB files
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    cols = [info[1] for info in cursor.fetchall()]
    if 'last_filing' not in cols: cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN last_filing TEXT')
    if 'filing_url' not in cols: cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN filing_url TEXT')

    # 2. GET MASTER LIST
    print("Fetching master ticker list...")
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    
    # To prevent 20-minute runs in GitHub Actions, let's process a subset 
    # OR you can process all. For this script, we'll iterate through all.
    tickers_to_process = list(master_data.values())[:100] # Set to [:100] for testing, remove for full list

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in tickers_to_process:
        cik = str(item['cik_str']).zfill(10)
        ticker = item['ticker'].upper()
        name = item['title']
        
        # INSERT placeholder if not exists
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, last_filing, filing_url, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (int(cik), ticker, name, "-", "2026-05-15 (Legal Fallback)", "Checking...", "", now))

        # 3. FETCH HISTORICAL DATA FROM SEC SUBMISSIONS API
        try:
            # Rate limit protection (10 requests per second max)
            time.sleep(0.15) 
            
            sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
            response = requests.get(sub_url, headers=HEADERS)
            
            if response.status_code == 200:
                data = response.json()
                filings = data.get('filings', {}).get('recent', {})
                
                # Look for the most recent 10-K or 10-Q
                found = False
                for i, form in enumerate(filings.get('form', [])):
                    if form in ['10-K', '10-Q']:
                        acc_num = filings['accessionNumber'][i].replace('-', '')
                        primary_doc = filings['primaryDocument'][i]
                        report_date = filings['reportDate'][i]
                        
                        f_link = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_num}/{primary_doc}"
                        f_info = f"{form} ({report_date})"
                        
                        cursor.execute('''
                            UPDATE ticker_event_log 
                            SET last_filing = ?, filing_url = ?, timestamp = ?
                            WHERE ticker = ?
                        ''', (f_info, f_link, now, ticker))
                        found = True
                        break 
                if not found:
                    cursor.execute("UPDATE ticker_event_log SET last_filing = 'None Found' WHERE ticker = ?", (ticker,))
        except Exception as e:
            print(f"Error updating {ticker}: {e}")

    conn.commit()

    # 4. EXPORT
    cursor.execute("SELECT ticker, name, next_earnings, last_filing, filing_url FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Earnings', 'Last 10-K/Q', 'Filing Link'])
        writer.writerows(rows)
    
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
