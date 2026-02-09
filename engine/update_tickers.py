import requests
import sqlite3
import csv
import time
import os
from datetime import datetime

# --- PATHS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, 'tickers.db')
CSV_OUTPUT = os.path.join(BASE_DIR, 'tickers_preview.csv')

USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}

def main():
    print(f"[{datetime.now()}] Initializing SEC Engine with Revenue Support...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. UPDATED TABLE (Added Revenue)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            cik INTEGER UNIQUE, ticker TEXT, name TEXT, industry TEXT,
            location TEXT, incorporated TEXT, fye TEXT,
            last_10k TEXT, link_10k TEXT, revenue REAL, timestamp DATETIME
        )
    ''')
    
    # Ensure the revenue column exists if the table was already created
    try:
        cursor.execute("ALTER TABLE ticker_event_log ADD COLUMN revenue REAL")
    except:
        pass

    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    tickers = list(master_data.values())

    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        if i % 100 == 0:
            print(f"Processing {i}/{len(tickers)}: {ticker}")

        try:
            time.sleep(0.12) # Stay under 10 requests/sec limit
            
            # --- CALL 1: SUBMISSIONS (For Links & Metadata) ---
            sub_resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik_str}.json", headers=HEADERS).json()
            
            # --- CALL 2: COMPANY FACTS (For Revenue) ---
            # We fetch the specific "Revenues" tag from the SEC's XBRL data
            facts_resp = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json", headers=HEADERS).json()
            
            # REVENUE EXTRACTION LOGIC
            revenue = 0
            # We try the three most common SEC revenue tags in order of reliability
            for tag in ['Revenues', 'SalesRevenueNet', 'RevenueFromContractWithCustomerExcludingAssessedTax']:
                try:
                    # Get the 'USD' entries for the tag and take the most recent value
                    rev_entries = facts_resp['facts']['us-gaap'][tag]['units']['USD']
                    # Sort by 'end' date to get the latest annual/quarterly number
                    revenue = sorted(rev_entries, key=lambda x: x['end'])[-1]['val']
                    if revenue: break 
                except KeyError:
                    continue

            # METADATA & FILINGS (Existing logic)
            industry = sub_resp.get('sicDescription', 'N/A')
            biz = sub_resp.get('addresses', {}).get('business', {})
            location = f"{biz.get('city', '')}, {biz.get('stateProvince', '')}".strip(", ")
            
            recent = sub_resp.get('filings', {}).get('recent', {})
            k_date, k_link = "N/A", ""
            if '10-K' in recent.get('form', []):
                idx = recent['form'].index('10-K')
                acc = recent['accessionNumber'][idx].replace('-', '')
                doc = recent['primaryDocument'][idx]
                k_date = recent['reportDate'][idx]
                k_link = f"https://www.sec.gov/Archives/edgar/data/{int(cik_str)}/{acc}/{doc}"

            # SAVE TO DB
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, industry, location, last_10k, link_10k, revenue, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cik) DO UPDATE SET
                    ticker=excluded.ticker, industry=excluded.industry, revenue=excluded.revenue,
                    last_10k=excluded.last_10k, link_10k=excluded.link_10k, timestamp=excluded.timestamp
            ''', (int(cik_str), ticker, item['title'], industry, location, k_date, k_link, revenue, datetime.now()))
            
            if i % 100 == 0: conn.commit()

        except Exception as e:
            continue

    # 4. EXPORT TO CSV (Added Revenue to Header)
    cursor.execute("SELECT Ticker, Name, Location, Industry, Revenue, last_10k, link_10k FROM ticker_event_log ORDER BY revenue DESC")
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Location', 'Industry', 'Revenue', '10K_Date', '10K_Link'])
        writer.writerows(cursor.fetchall())
    
    conn.commit()
    conn.close()
    print("Engine Finished. CSV and DB now contain Revenue data.")

if __name__ == "__main__":
    main()
