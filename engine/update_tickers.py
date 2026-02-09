import requests
import sqlite3
import csv
import time
import sys
import os
from datetime import datetime

# --- AUTOMATIC PATH ROUTING ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'tickers.db')
CSV_OUTPUT = os.path.join(BASE_DIR, 'tickers_preview.csv')

USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}

# --- BULLETPROOF FILING DEFINITIONS ---
# Includes Foreign Private Issuers (20-F), Canadian (40-F), and Amendments (/A)
ANNUAL_FORMS = {'10-K', '20-F', '40-F', '10-K/A', '20-F/A', '40-F/A'}
QUARTERLY_FORMS = {'10-Q', '6-K', '10-Q/A', '6-K/A'}

def main():
    print(f"[{datetime.now()}] Initializing Bulletproof SEC Sync...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. DATABASE SETUP (Including Metadata)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT,
            sic TEXT, industry TEXT,
            location TEXT, incorporated TEXT, fye TEXT,
            last_10k TEXT, link_10k TEXT,
            last_10q TEXT, link_10q TEXT,
            timestamp DATETIME
        )
    ''')
    
    # 2. FETCH MASTER LIST
    try:
        master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
        tickers = list(master_data.values())
    except Exception as e:
        print(f"Failed to fetch master list: {e}")
        return

    # 3. SYNC LOOP
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        if i % 100 == 0:
            sys.stdout.write(f"\rProcessing: {i}/{len(tickers)} ({ticker})")
            sys.stdout.flush()

        try:
            time.sleep(0.11) # SEC rate limit: 10 requests/sec
            resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik_str}.json", headers=HEADERS)
            
            if resp.status_code == 200:
                data = resp.json()
                
                # METADATA EXTRACTION
                sic = data.get('sic', 'N/A')
                industry = data.get('sicDescription', 'N/A')
                incorp = data.get('stateOfIncorporation', 'N/A')
                fye = data.get('fiscalYearEnd', 'N/A')
                
                biz = data.get('addresses', {}).get('business', {})
                location = f"{biz.get('city', '')}, {biz.get('stateProvince', '')}".strip(", ")

                # FILING LOGIC (The Waterfall)
                recent = data.get('filings', {}).get('recent', {})
                forms = recent.get('form', [])
                dates = recent.get('reportDate', [])
                accs = recent.get('accessionNumber', [])
                docs = recent.get('primaryDocument', [])

                # DANGER FIX: Create a list of tuples and sort by date descending
                # This ensures we get the newest report even if it's listed out of order
                filing_tuples = []
                for j in range(len(forms)):
                    filing_tuples.append({
                        'form': forms[j],
                        'date': dates[j],
                        'acc': accs[j].replace('-', ''),
                        'doc': docs[j]
                    })
                
                # Sort by date (Y-M-D strings sort correctly)
                filing_tuples.sort(key=lambda x: x['date'], reverse=True)

                k_date, k_link, q_date, q_link = "N/A", "", "N/A", ""

                for f in filing_tuples:
                    link = f"https://www.sec.gov/Archives/edgar/data/{int(cik_str)}/{f['acc']}/{f['doc']}"
                    
                    # Catch Annuals (10-K, 20-F, 40-F)
                    if f['form'] in ANNUAL_FORMS and k_date == "N/A":
                        k_date, k_link = f['date'], link
                    
                    # Catch Quarterlies (10-Q, 6-K)
                    if f['form'] in QUARTERLY_FORMS and q_date == "N/A":
                        q_date, q_link = f['date'], link
                        
                    if k_date != "N/A" and q_date != "N/A":
                        break

                # SAVE TO DB
                cursor.execute('''
                    INSERT INTO ticker_event_log (cik, ticker, name, sic, industry, location, incorporated, fye, last_10k, link_10k, last_10q, link_10q, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cik) DO UPDATE SET
                        ticker=excluded.ticker, name=excluded.name, sic=excluded.sic, industry=excluded.industry,
                        location=excluded.location, incorporated=excluded.incorporated, fye=excluded.fye,
                        last_10k=excluded.last_10k, link_10k=excluded.link_10k, 
                        last_10q=excluded.last_10q, link_10q=excluded.link_10q, timestamp=excluded.timestamp
                ''', (int(cik_str), ticker, item['title'], sic, industry, location, incorp, fye, k_date, k_link, q_date, q_link, now))
            
            if i % 500 == 0: conn.commit()
        except Exception:
            continue

    conn.commit()
    
    # 4. EXPORT TO CSV
    cursor.execute("SELECT ticker, name, location, incorporated, fye, industry, last_10k, link_10k, last_10q, link_10q FROM ticker_event_log ORDER BY ticker ASC")
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Location', 'Inc', 'FYE', 'Industry', '10K_Date', '10K_Link', '10Q_Date', '10Q_Link'])
        writer.writerows(cursor.fetchall())
    
    conn.close()
    print(f"\n[{datetime.now()}] Sync Complete. CSV Generated.")

if __name__ == "__main__":
    main()
