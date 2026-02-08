import requests
import sqlite3
import csv
import sys

# 1. Setup Database
db_file = 'tickers.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# We use CIK as the PRIMARY KEY so we never get duplicates
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sec_tickers (
        cik INTEGER PRIMARY KEY,
        ticker TEXT,
        name TEXT,
        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# 2. Fetch Data from SEC
# SEC requires a real-looking User-Agent or they return a 403 Forbidden error
headers = {
    'User-Agent': 'RyanSchraub (ryan.schraub@gmail.com)',
    'Accept-Encoding': 'gzip, deflate'
}
url = "https://www.sec.gov/files/company_tickers.json"

print("Contacting SEC.gov...")
try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    # 3. Process and Upsert Data
    # 'Upsert' means: If CIK exists, update it. If not, insert it.
    ticker_data = []
    for entry in data.values():
        ticker_data.append((entry['cik_str'], entry['ticker'], entry['title']))

    cursor.executemany('''
        INSERT INTO sec_tickers (cik, ticker, name)
        VALUES (?, ?, ?)
        ON CONFLICT(cik) DO UPDATE SET
            ticker=excluded.ticker,
            name=excluded.name,
            last_updated=CURRENT_TIMESTAMP
    ''', ticker_data)
    
    conn.commit()
    print(f"Successfully processed {len(ticker_data)} tickers.")

    # 4. Export to CSV for GitHub Preview
    # This creates the file that GitHub shows as a table in your browser
    cursor.execute("SELECT ticker, cik, name FROM sec_tickers ORDER BY ticker ASC")
    rows = cursor.fetchall()
    
    with open('tickers_preview.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Ticker', 'CIK', 'Company Name'])
        writer.writerows(rows)
    
    print("CSV preview file generated.")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1) # Fail the GitHub Action if the script fails
finally:
    conn.close()
