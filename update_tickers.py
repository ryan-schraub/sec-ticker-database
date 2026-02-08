import requests
import sqlite3
import os

# 1. Setup Database
db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sec_tickers (
        cik INTEGER PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# 2. Fetch Data from SEC
# IMPORTANT: Use a real email/name or the SEC will block you
headers = {'User-Agent': 'Ryan Schraub ryan.schraub@gmail.com'}
url = "https://www.sec.gov/file/company-tickers"

print("Fetching data from SEC...")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # 3. Upsert into SQLite
    for entry in data.values():
        cursor.execute('''
            INSERT INTO sec_tickers (cik, ticker, name)
            VALUES (?, ?, ?)
            ON CONFLICT(cik) DO UPDATE SET
                ticker=excluded.ticker,
                name=excluded.name,
                last_updated=CURRENT_TIMESTAMP
        ''', (entry['cik_str'], entry['ticker'], entry['title']))
    
    conn.commit()
    print(f"Successfully updated {len(data)} tickers.")

except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
