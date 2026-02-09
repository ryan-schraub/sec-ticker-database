import sqlite3
import requests
import time
import os

# --- CONFIGURATION ---
# Use the same headers as your main sync to avoid SEC blocks
USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}
DB_PATH = 'tickers.db'

# Priority list for revenue tags - Modern accounting standard first
TAG_WATERFALL = [
    ('us-gaap', 'RevenueFromContractWithCustomerExcludingAssessedTax'),
    ('us-gaap', 'SalesRevenueNet'),
    ('us-gaap', 'Revenues'),
    ('ifrs-full', 'RevenueFromContractsWithCustomers'), # International Support
    ('us-gaap', 'SalesRevenueGoodsNet'),
    ('us-gaap', 'InterestAndDividendIncomeOperating') # Financials/Banks
]

def get_revenue_from_facts(cik):
    """Fetches the latest Annual Revenue from SEC CompanyFacts."""
    padded_cik = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{padded_cik}.json"
    
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200: 
            return None
        data = response.json()
        
        for namespace, tag in TAG_WATERFALL:
            try:
                # 1. Check for the tag in the JSON
                units_dict = data['facts'][namespace][tag]['units']
                
                # 2. Handle Currency (USD first, then others for foreign issuers)
                currency = 'USD' if 'USD' in units_dict else list(units_dict.keys())[0]
                points = units_dict[currency]
                
                # 3. Filter for Annual Data (Fiscal Year 'FY')
                # This prevents accidentally grabbing a single quarter's revenue.
                annual_points = [p for p in points if p.get('fp') == 'FY']
                
                if not annual_points:
                    # Fallback to latest available if 'FY' tag is missing
                    latest = sorted(points, key=lambda x: x['end'], reverse=True)[0]
                else:
                    latest = sorted(annual_points, key=lambda x: x['end'], reverse=True)[0]
                
                return latest['val']
            except (KeyError, IndexError):
                continue
    except Exception as e:
        print(f"Error fetching CIK {cik}: {e}")
    return None

def run_enrichment():
    """Updates the database with revenue figures."""
    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Ensure Revenue column exists
    try:
        cursor.execute("ALTER TABLE ticker_event_log ADD COLUMN revenue REAL")
    except sqlite3.OperationalError:
        pass # Column already exists

    # 2. Fetch tickers that need data
    # We prioritize tickers with 0 or NULL revenue first
    cursor.execute("SELECT ticker, cik FROM ticker_event_log ORDER BY revenue ASC")
    rows = cursor.fetchall()
    
    print(f"Starting enrichment for {len(rows)} tickers...")
    
    success_count = 0
    for i, (ticker, cik) in enumerate(rows):
        rev = get_revenue_from_facts(cik)
        
        if rev:
            cursor.execute("UPDATE ticker_event_log SET revenue = ? WHERE ticker = ?", (rev, ticker))
            print(f"[{i+1}/{len(rows)}] ✓ {ticker}: ${rev:,.0f}")
            success_count += 1
        else:
            print(f"[{i+1}/{len(rows)}] ✗ {ticker}: No revenue found")
        
        # 3. Frequent Commits: Save every 50 tickers to prevent data loss
        if i % 50 == 0:
            conn.commit()
        
        # 4. Respect SEC rate limit (10 requests per second)
        time.sleep(0.12) 
        
    conn.commit()
    conn.close()
    print(f"\nEnrichment Complete. Successfully updated {success_count} tickers.")

if __name__ == "__main__":
    run_enrichment()
