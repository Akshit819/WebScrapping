import requests
import pandas as pd
import time
from datetime import datetime, timedelta


def format_date_for_api(date_str):
    """Convert DD-MM-YYYY to YYYYMMDD format for API."""
    day, month, year = date_str.split('-')
    return f"{year}{month.zfill(2)}{day.zfill(2)}"

def create_pdf_url(attachment_name):
    """Create the correct BSE PDF URL."""
    if attachment_name:
        return f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attachment_name}"
    return ""

def get_url(page_num, api_from_date, api_to_date, category):
    return (
        f"https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        f"?pageno={page_num}&strCat={category}&strPrevDate={api_from_date}"
        f"&strScrip=&strSearch=P&strToDate={api_to_date}&strType=C&subcategory=-1"
    )


def get_bse_csv(from_date, to_date, category):
    """
    Extract BSE announcements and create CSV with company name and PDF link.
    
    Args:
        from_date: DD-MM-YYYY (e.g., '24-07-2025')
        to_date: DD-MM-YYYY (e.g., '25-07-2025')
        category: e.g., 'Result', 'Board Meeting'
    """
    print(f"🔍 Extracting {category} announcements from {from_date} to {to_date}...")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.bseindia.com/'
    }

    api_from_date = format_date_for_api(from_date)
    api_to_date = format_date_for_api(to_date)

    all_companies = []
    page_num = 1

    while page_num <= 20:
        try:
            print(f"📄 Fetching page {page_num}...")
            url = get_url(page_num, api_from_date, api_to_date, category)
            time.sleep(1)  # Respectful delay to avoid hitting API too fast
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            announcements = data.get('Table', [])

            if not announcements:
                print(f"⚠️ No data on page {page_num}. Ending.")
                break

            for item in announcements:
                company_name = item.get('SLONGNAME', '')
                attachment_name = item.get('ATTACHMENTNAME', '')
                scrip_cd = item.get('SCRIP_CD', '')

                if company_name and attachment_name:
                    pdf_url = create_pdf_url(attachment_name)
                    all_companies.append({
                        'Company_Name': company_name,
                        'Scrip_CD': scrip_cd,
                        'PDF_Link': pdf_url
                    })

            print(f"✅ Found {len(announcements)} announcements on page {page_num}")
            page_num += 1

        except Exception as e:
            print(f"❌ Error on page {page_num}: {e}")
            break

    if all_companies:
        df = pd.DataFrame(all_companies)
        safe_category = category.replace('/', '_').replace(' ', '_')
        filename = f"{safe_category}_{from_date.replace('-', '')}_{to_date.replace('-', '')}.csv"

        df.to_csv(filename, index=False)

        print(f"\n🎉 SUCCESS! Created: {filename}")
        print(f"📊 Total companies: {len(df)}")
        print(f"🔎 Sample data:\n{df.head(5).to_string(index=False)}")
        return filename
    else:
        print("⚠️ No announcements found!")
        return None

# Run if main
if __name__ == "__main__":
    print("=== 🧪 Testing Extraction ===")
    get_bse_csv("24-07-2025", "25-07-2025", "Result")
