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
RSS_URL = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom"

def main():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # 1. SETUP TABLE: Keeps your original ticker list structure
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

    # 2. PART A: THE MASTER LIST (All Tickers)
    # This runs every time but 'INSERT OR IGNORE' ensures it doesn't overwrite your data
    master_data = requests.get("https://www.sec.gov/files/company_tickers.json", headers=HEADERS).json()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for item in master_data.values():
        cik, ticker, name = item['cik_str'], item['ticker'].upper(), item['title']
        cursor.execute('''
            INSERT OR IGNORE INTO ticker_event_log (cik, ticker, name, event_scenario, next_earnings, timestamp) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (cik, ticker, name, "-", "2026-05-15 (Legal Fallback)", now))

    # 3. PART B: THE AI RESEARCH (Hourly Pulse)
    # This specifically looks for new 8-Ks and updates the 'Earnings' for those tickers
    try:
        rss = requests.get(RSS_URL, headers=HEADERS)
        root = ET.fromstring(rss.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            if "8-K" in title:
                # Extracts ticker from "8-K - Apple Inc. (0000320193) (Ticker: AAPL)"
                try:
                    ticker_part = title.split('(')[-1].replace("Ticker: ", "").replace(")", "").strip()
                    
                    # Update ONLY the earnings info for this specific company
                    cursor.execute('''
                        UPDATE ticker_event_log 
                        SET next_earnings = ?, event_scenario = ?, timestamp = ?
                        WHERE ticker = ?
                    ''', ("Confirmed via 8-K", "AI_RESEARCHED", now, ticker_part))
                except: continue
    except Exception as e:
        print(f"Research failed: {e}")

    conn.commit()

    # 4. EXPORT TO CSV: For the website to display
    cursor.execute("SELECT ticker, name, event_scenario, next_earnings, timestamp FROM ticker_event_log ORDER BY ticker ASC")
    rows = cursor.fetchall()
    with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'Company', 'Status', 'Earnings', 'LastSync'])
        writer.writerows(rows)
    conn.close()

if __name__ == "__main__":
    main()
