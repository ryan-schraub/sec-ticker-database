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

def fetch_with_retry(url, retries=3, backoff=2):
    """Handles transient 500 errors seen in logs."""
    for i in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
        except Exception:
            pass
        time.sleep(backoff * (i + 1))
    return None

def main():
    print(f"[{datetime.now()}] Initializing SEC Intelligence Engine...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. DATABASE SETUP
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT, 
            industry TEXT,
            location TEXT, 
            last_10k TEXT, 
            link_10k TEXT, 
            revenue REAL, 
            timestamp DATETIME
        )
    ''')
    
    # 2. FETCH TICKER LIST
    master_resp = fetch_with_retry("https://www.sec.gov/files/company_tickers.json")
    if not master_resp:
        print("Failed to fetch master ticker list. Exiting.")
        return
    
    tickers = list(master_resp.values())
    print(f"Total Tickers to process: {len(tickers)}")

    for i, item in enumerate(tickers):
        cik_str = str(item['cik_str']).zfill(10)
        ticker = str(item['ticker']).upper()
        
        # Rate limit compliance (approx 8-9 requests per second)
        time.sleep(0.12) 

        try:
            # --- CALL 1: SUBMISSIONS ---
            sub_resp = fetch_with_retry(f"https://data.sec.gov/submissions/CIK{cik_str}.json")
            if not sub_resp: continue

            # --- CALL 2: COMPANY FACTS ---
            facts_resp = fetch_with_retry(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json")
            
            # REVENUE EXTRACTION WATERFALL (Expanded for Banks/Finance)
            revenue = 0
            if facts_resp and 'facts' in facts_resp:
                tags = [
                    ('us-gaap', 'RevenueFromContractWithCustomerExcludingAssessedTax'),
                    ('us-gaap', 'SalesRevenueNet'),
                    ('us-gaap', 'Revenues'),
                    ('us-gaap', 'InterestIncomeNet'), # For Banks
                    ('us-gaap', 'GrossProfit'),      # Alternative
                    ('ifrs-full', 'RevenueFromContractsWithCustomers')
                ]

                for namespace, tag in tags:
                    try:
                        if namespace in facts_resp['facts'] and tag in facts_resp['facts'][namespace]:
                            units = facts_resp['facts'][namespace][tag]['units']
                            currency = 'USD' if 'USD' in units else list(units.keys())[0]
                            points = units[currency]
                            
                            # Filter for Annual Data (FY)
                            annual_points = [p for p in points if p.get('fp') == 'FY']
                            source_list = annual_points if annual_points else points
                            
                            if source_list:
                                revenue = sorted(source_list, key=lambda x: x['end'])[-1]['val']
                            
                            if revenue: break 
                    except (KeyError, IndexError):
                        continue

            # METADATA EXTRACTION
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

            # 3. SAVE TO DB
            cursor.execute('''
                INSERT INTO ticker_event_log (cik, ticker, name, industry, location, last_10k, link_10k, revenue, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cik) DO UPDATE SET
                    ticker=excluded.ticker, 
                    industry=excluded.industry, 
                    revenue=excluded.revenue,
                    last_10k=excluded.last_10k, 
                    link_10k=excluded.link_10k, 
                    timestamp=excluded.timestamp
            ''', (int(cik_str), ticker, item['title'], industry, location, k_date, k_link, revenue, datetime.now()))
            
            if i % 100 == 0:
                print(f"Processed {i}/{len(tickers)}: {ticker} | Revenue: ${revenue:,.0f}")
                conn.commit() # Commit every 100 to prevent data loss on timeout

        except Exception as e:
            # Logging error but continuing to next ticker
            continue

    # 4. EXPORT TO CSV
    print("\nGenerating CSV for frontend...")
    cursor.execute("""
        SELECT ticker, name, location, industry, revenue, last_10k, link_10k 
        FROM ticker_event_log 
        WHERE ticker IS NOT NULL
        ORDER BY revenue DESC
    """)
    
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Location', 'Industry', 'Revenue', '10K_Date', '10K_Link'])
        writer.writerows(cursor.fetchall())
    
    conn.commit()
    conn.close()
    print(f"[{datetime.now()}] Engine Finished. {len(tickers)} tickers synced.")

if __name__ == "__main__":
    main()
