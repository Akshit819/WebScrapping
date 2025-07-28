import requests
import pandas as pd
import time
from datetime import datetime
import os

def format_date_for_api(date_str):
    day, month, year = date_str.split('-')
    return f"{year}{month.zfill(2)}{day.zfill(2)}"

def create_pdf_url(attachment_name):
    if attachment_name:
        return f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attachment_name}"
    return ""

def get_url(page_num, api_from_date, api_to_date, category):
    return (
        f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        f"?pageno={page_num}&strCat={category}&strPrevDate={api_from_date}"
        f"&strScrip=&strSearch=P&strToDate={api_to_date}&strType=C&subcategory=-1"
    )

def fetch_announcements(from_date, to_date, category):
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.bseindia.com/'
    }

    api_from_date = format_date_for_api(from_date)
    api_to_date = format_date_for_api(to_date)
    page_num = 1
    announcements = []

    while page_num <= 20:
        try:
            url = get_url(page_num, api_from_date, api_to_date, category)
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            table = data.get('Table', [])

            if not table:
                break

            for item in table:
                company_name = item.get('SLONGNAME', '')
                attachment_name = item.get('ATTACHMENTNAME', '')
                scrip_cd = item.get('SCRIP_CD', '')
                timestamp = item.get('NEWS_DT', '')

                if company_name and attachment_name:
                    pdf_url = create_pdf_url(attachment_name)
                    announcements.append({
                        'Company_Name': company_name,
                        'Scrip_CD': scrip_cd,
                        'PDF_Link': pdf_url,
                        'Timestamp': timestamp
                    })

            page_num += 1

        except Exception as e:
            print(f"❌ Error fetching page {page_num}: {e}")
            break

    return announcements

def monitor_bse(category="Result", polling_minutes=1):
    print(f"📡 Starting live BSE monitor for category: {category}")
    today = datetime.now().strftime("%d-%m-%Y")
    filename = f"live_{category.replace(' ', '_')}.csv"

    if os.path.exists(filename):
        df_existing = pd.read_csv(filename)
        seen_links = set(df_existing['PDF_Link'])
    else:
        df_existing = pd.DataFrame(columns=["Company_Name", "Scrip_CD", "PDF_Link", "Timestamp"])
        seen_links = set()

    while True:
        print(f"\n⏰ Checking for new announcements at {datetime.now().strftime('%H:%M:%S')}")
        new_data = fetch_announcements(today, today, category)

        if not new_data:
            print("🔁 No announcements found.")
        else:
            new_rows = [row for row in new_data if row['PDF_Link'] not in seen_links]

            if new_rows:
                print(f"🆕 Found {len(new_rows)} new announcement(s)!")

                # 🖨️ Print new announcements to terminal
                for row in new_rows:
                    print(
                        f"\n📢 NEW ANNOUNCEMENT\n"
                        f"Company: {row['Company_Name']}\n"
                        f"Scrip Code: {row['Scrip_CD']}\n"
                        f"Link: {row['PDF_Link']}\n"
                        f"Timestamp: {row['Timestamp']}\n"
                        f"{'-'*40}"
                    )

                df_new = pd.DataFrame(new_rows)
                df_new.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)

                # Update the seen links
                seen_links.update([row['PDF_Link'] for row in new_rows])
            else:
                print("📭 No new announcements since last check.")

        print(f"🛌 Sleeping for {polling_minutes} minute(s)...")
        time.sleep(polling_minutes * 60)

if __name__ == "__main__":
    monitor_bse(category="Result", polling_minutes=1)
