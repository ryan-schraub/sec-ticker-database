import requests
import sqlite3
import csv
import time
import sys
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
            cik INTEGER UNIQUE, ticker TEXT, name TEXT,
            last_10k TEXT, link_10k TEXT,
            last_10q TEXT, link_10q TEXT,
            timestamp DATETIME
        )
    ''')
    
    # Force add columns if they don't exist (Fixes OperationalError)
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    existing_cols = [info[1] for info in cursor.fetchall()]
    for col in ['last_10k', 'link_10k', 'last_10q', 'link_10q']:
        if col not in existing_cols:
            cursor.execute(f'ALTER TABLE ticker_event_log ADD COLUMN {col} TEXT DEFAULT "N/A"')

    # 2. FETCH MASTER LIST
    print(f"[{datetime.now()}] Fetching SEC Master Ticker List...")
    try:
        master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
        tickers = list(master_data.values())
    except Exception as e:
        print(f"Failed to fetch master list: {e}")
        return

    total = len(tickers)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Syncing {total} tickers. This will take ~28 minutes.")

    # 3. HISTORICAL LOOKUP LOOP
    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        # simplified progress display to prevent formatting errors
        if i % 50 == 0:
            sys.stdout.write(f"\rProgress: {i}/{total} ({ticker}) ")
            sys.stdout.flush()

        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, timestamp) 
            VALUES (?, ?, ?, ?)
        ''', (int(cik_str), ticker, item['title'], now))

        try:
            time.sleep(0.12) # Stay under 10 req/sec
            resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik_str}.json", headers=HEADERS)
            
            if resp.status_code == 200:
                data = resp.json()
                recent = data.get('filings', {}).get('recent', {})
                
                k_date, k_link = "N/A", ""
                q_date, q_link = "N/A", ""

                for j, form in enumerate(recent.get('form', [])):
                    acc = recent['accessionNumber'][j].replace('-', '')
                    doc = recent['primaryDocument'][j]
                    date = recent['reportDate'][j]
                    link = f"https://www.sec.gov/Archives/edgar/data/{int(cik_str)}/{acc}/{doc}"

                    if form == '10-K' and k_date == "N/A":
                        k_date, k_link = date, link
                    elif form == '10-Q' and q_date == "N/A":
                        q_date, q_link = date, link
                    
                    if k_date != "N/A" and q_date != "N/A":
                        break
                
                cursor.execute('''
                    UPDATE ticker_event_log 
                    SET last_10k = ?, link_10k = ?, last_10q = ?, link_10q = ?, timestamp = ?
                    WHERE ticker = ?
                ''', (k_date, k_link, q_date, q_link, now, ticker))
            
            if i % 500 == 0: conn.commit()
        except: continue

    conn.commit()
    
    # 4. EXPORT TO CSV
    cursor.execute("SELECT ticker, name, last_10k, link_10k, last_10q, link_10q FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Latest 10-K', '10-K Link', 'Latest 10-Q', '10-Q Link'])
        writer.writerows(rows)
    
    conn.close()
    print(f"\n[{datetime.now()}] Update Complete.")

if __name__ == "__main__":
    main()
