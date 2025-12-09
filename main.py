import requests
import pandas as pd
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

headers = {
    "Host": "api.wallchain.xyz",
    "Connection": "keep-alive",
    "sec-ch-ua-platform": "Linux",
    "Accept": "application/json, text/plain, */*"
}

companies_url = 'https://api.wallchain.xyz/voices/companies/cards'
r = requests.get(companies_url, headers=headers)
company_ids = [item["companyId"] for item in r.json()]

csv_file = "wallchain_data5.csv"
if os.path.exists(csv_file):
    df_existing = pd.read_csv(csv_file)
    print(f"Loaded existing CSV with {len(df_existing)} rows")
else:
    df_existing = pd.DataFrame()
    print("No existing CSV found. Starting fresh.")

all_data = []

def fetch_page(company, period, page, asc_value):
    url = (
        f"https://api.wallchain.xyz/voices/companies/{company}/leaderboard"
        f"?page={page}&pageSize=20&orderBy=position"
        f"&ascending={asc_value}&period={period}"
    )

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"Non-200 response for {company}|{period}|{page}|{asc_value}: {r.status_code}")
        return []

    page_data = r.json()
    if "entries" not in page_data or not page_data["entries"]:
        print(f"No entries for {company}|{period}|{page}|{asc_value}")
        return []

    rows = []
    for entry in page_data["entries"]:
        if not df_existing.empty and ((df_existing['project'] == company) & (df_existing['username'] == entry["xInfo"]["username"])).any():
            continue

        row = {
            "project": company,
            "period": period,
            "position": entry.get("position"),
            "positionChange": entry.get("positionChange"),
            "mindsharePercentage": entry.get("mindsharePercentage"),
            "relativeMindshare": entry.get("relativeMindshare"),
            "id": entry["xInfo"].get("id"),
            "name": entry["xInfo"].get("name"),
            "rank": entry["xInfo"].get("rank"),
            "score": entry["xInfo"].get("score"),
            "scorePercentile": entry["xInfo"].get("scorePercentile"),
            "scoreQuantile": entry["xInfo"].get("scoreQuantile"),
            "username": entry["xInfo"].get("username")
        }
        rows.append(row)

    print(f"Completed {company}|{period}|{page}|{asc_value}")
    return rows


try:
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = []
        for company in company_ids:
            for period in ["30d", "7d", "epoch-1", "epoch-2"]:
                for ascending in [False, True]:
                    asc_value = "true" if ascending else "false"
                    for page in range(1, 51):
                        futures.append(
                            executor.submit(
                                fetch_page,
                                company,
                                period,
                                page,
                                asc_value
                            )
                        )

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)

except KeyboardInterrupt:
    print("Scraper stopped by user. Saving data...")

if all_data:
    df_new = pd.DataFrame(all_data)
    if not df_existing.empty:
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new
    df_final.to_csv(csv_file, index=False)
    print(f"Saved CSV with {len(df_final)} rows")
