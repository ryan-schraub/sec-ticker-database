import requests
import sqlite3
import csv
import os

# 1. Setup Database
# This creates a file named 'tickers.db' in your GitHub repo
db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create the table with CIK as the Unique ID
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sec_tickers (
        cik INTEGER PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# 2. Fetch Data from SEC
# The SEC requires a User-Agent that identifies you
headers = {
    'User-Agent': 'Ryan Schraub (ryan.schraub@gmail.com)',
    'Accept-Encoding': 'gzip, deflate'
}
url = "https://www.sec.gov/file/company-tickers"

print("Contacting SEC for latest ticker list...")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # 3. Upsert into SQLite (Update existing, Insert new)
    # We loop through the SEC's JSON and save it to our table
    rows_to_insert = []
    for item in data.values():
        rows_to_insert.append((item['cik_str'], item['ticker'], item['title']))

    cursor.executemany('''
        INSERT INTO sec_tickers (cik, ticker, name)
        VALUES (?, ?, ?)
        ON CONFLICT(cik) DO UPDATE SET
            ticker=excluded.ticker,
            name=excluded.name,
            last_updated=CURRENT_TIMESTAMP
    ''', rows_to_insert)
    
    conn.commit()
    print(f"Database updated successfully with {len(rows_to_insert)} tickers.")

    # 4. Generate the CSV for GitHub Preview
    # This part ensures 'tickers_preview.csv' is always created/overwritten
    cursor.execute("SELECT ticker, cik, name FROM sec_tickers ORDER BY ticker ASC")
    all_tickers = cursor.fetchall()
    
    csv_file = 'tickers_preview.csv'
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Company Name']) # Header row
        writer.writerows(all_tickers)
    
    print(f"CSV preview generated: {csv_file}")

except Exception as e:
    print(f"An error occurred: {e}")
    # This prevents GitHub from thinking the run succeeded if it actually failed
    exit(1) 
finally:
    conn.close()
