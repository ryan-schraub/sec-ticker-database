import requests
import sqlite3
import csv
import xml.etree.ElementTree as ET
from datetime import datetime

# --- CONFIG ---
DB_FILE = 'tickers.db'
CSV_OUTPUT = 'tickers_preview.csv'
USER_EMAIL = "ryan.schraub@gmail.com" 
HEADERS = {'User-Agent': f'RyanBot ({USER_EMAIL})'}
RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. CREATE TABLE & UPDATE SCHEMA
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_event_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik INTEGER UNIQUE, 
            ticker TEXT, 
            name TEXT,
            event_scenario TEXT, 
            next_earnings TEXT, 
            timestamp DATETIME
        )
    ''')

    # Migration: Add 'last_filing' and 'filing_url' if they are missing from the old DB
    cursor.execute("PRAGMA table_info(ticker_event_log)")
    columns = [info[1] for info in cursor.fetchall()]
    
    if 'last_filing' not in columns:
        cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN last_filing TEXT DEFAULT "None Found"')
    if 'filing_url' not in columns:
        cursor.execute('ALTER TABLE ticker_event_log ADD COLUMN filing_url TEXT DEFAULT ""')

    # 2. PART A: THE MASTER LIST
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        # We include all columns here now that the migration ensures they exist
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, last_filing, filing_url, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (cik, ticker, name, "-", "2026-05-15 (Legal Fallback)", "None Found", "", now))

    # 3. PART B: 10-K / 10-Q FILING RESEARCH
    try:
        rss = requests.get(RSS_URL, headers=HEADERS)
        root = ET.fromstring(rss.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            if any(form in title for form in ["10-K", "10-Q"]):
                try:
                    ticker_part = title.split('(')[-1].replace("Ticker: ", "").replace(")", "").strip()
                    form_type = "10-K" if "10-K" in title else "10-Q"
                    
                    link_tag = entry.find('atom:link', ns)
                    filing_link = link_tag.attrib.get('href') if link_tag is not None else ""
                    
                    cursor.execute('''
                        UPDATE ticker_event_log 
                        SET last_filing = ?, filing_url = ?, timestamp = ?
                        WHERE ticker = ?
                    ''', (f"{form_type} ({now[:10]})", filing_link, now, ticker_part))
                except: continue
    except Exception as e:
        print(f"Research failed: {e}")

    conn.commit()

    # 4. EXPORT TO CSV
    cursor.execute("SELECT ticker, name, event_scenario, next_earnings, last_filing, filing_url, timestamp FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Earnings', 'Last 10-K/Q', 'Filing Link', 'LastSync'])
        writer.writerows(rows)
    conn.close()

if __name__ == "__main__":
    main()
