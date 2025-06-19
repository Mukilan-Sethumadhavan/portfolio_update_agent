import time, io, csv, logging, os, json
import requests
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from langchain_community.document_loaders import CSVLoader
from langchain_google_genai import GoogleGenerativeAI
from langchain_core.prompts import PromptTemplate

# === CONFIG ===
PHANTOM_API_KEY = "4bc5RrYLCBTapPWDChBZzETZecpneyIJor1e5VxEqaA"
GEMINI_API_KEY  = "AIzaSyDzRAIeFhEW6g8pMJ3w-gJgv726VJj_hwk"
SHEET_URL       = "https://docs.google.com/spreadsheets/d/16Y2vI689Su3gAifxEFHDRQ_RcmnmrWbRIC_npHuHbUE/edit"
PHANTOMS = {
    "company_url_finder": "4886747300437263",
    "employee_export":     "4383143640004809",
    "activity_explorer":   "3016437418660427",
}
HEADERS = {
    "X-Phantombuster-Key-1": PHANTOM_API_KEY,
    "Content-Type": "application/json"
}
DOWNLOAD_DIR = "phantom_results"

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
log = logging.getLogger("LinkedInScraper")

# --- Helpers ---
def get_sheet():
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        'credentials_linkedin.json',
        ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    )
    return gspread.authorize(creds).open_by_url(SHEET_URL).sheet1

def clear_and_write(sheet, company):
    sheet.clear()
    sheet.append_row(["company"])
    sheet.append_row([company.strip()])
    log.info(f"✅ Sheet updated for '{company}'")

def launch_agent(agent_id, payload=None):
    url = f"https://api.phantombuster.com/api/v1/agent/{agent_id}/launch"
    res = requests.post(url, headers=HEADERS, json=payload or {})
    res.raise_for_status()
    cid = res.json().get("data", {}).get("containerId")
    if not cid:
        raise Exception(f"No containerId returned for agent {agent_id}")
    log.info(f"✈ Launched agent {agent_id} (container {cid})")

def download_via_s3(agent_id):
    r = requests.get("https://api.phantombuster.com/api/v2/agents/fetch",
                     headers=HEADERS, params={"id": agent_id})
    r.raise_for_status()
    res = r.json()
    s3 = res.get("s3Folder")
    org = res.get("orgS3Folder")
    if not s3 or not org:
        raise Exception("Missing s3Folder/orgS3Folder—results not ready")
    url = f"https://phantombuster.s3.amazonaws.com/{org}/{s3}/result.csv"
    log.info(f"📥 Downloading CSV from S3: {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    path = os.path.join(DOWNLOAD_DIR, "latest.csv")
    with open(path, "wb") as f:
        f.write(resp.content)
    log.info("✅ CSV downloaded successfully")
    return path

def download_and_filter(company_term):
    raw_path = download_via_s3(PHANTOMS["activity_explorer"])
    df = pd.read_csv(raw_path)
    df_filtered = df[df['profileUrl'].str.contains(company_term, case=False, na=False)]
    if df_filtered.empty:
        raise Exception(f"No entries found for '{company_term}'")
    df_filtered['timestamp'] = pd.to_datetime(df_filtered['timestamp'])
    df_latest = df_filtered.sort_values('timestamp', ascending=False).head(5)
    filtered_path = os.path.join(DOWNLOAD_DIR, "filtered_latest.csv")
    df_latest.to_csv(filtered_path, index=False)
    log.info("✅ Filtered CSV created")
    return filtered_path

def analyze_with_gemini(path, company):
    docs = CSVLoader(file_path=path).load()
    llm = GoogleGenerativeAI(model="models/gemini-2.0-flash", google_api_key=GEMINI_API_KEY)
    prompt = PromptTemplate(
        input_variables=["context","company"],
        template="""
Analyzing LinkedIn activities for {company} from latest data:
{context}

Summarize:
1. Company updates
2. Product/news
3. Role changes
4. Noteworthy posts
"""
    )
    result = (prompt | llm).invoke({"context": docs, "company": company})
    log.info("🎯 Gemini analysis complete")
    return result

def delete_all_outputs():
    for pid in PHANTOMS.values():
        requests.delete("https://api.phantombuster.com/api/v2/agents/fetch-output",
                        headers=HEADERS, params={"id": pid})
    log.info("🗑 Cleanup done")

def get_company_linkedin_data(company_name: str):
    """
    Main function to get LinkedIn data for a company
    Returns standardized data format compatible with the main workflow
    """
    print(f"🔗 Processing LinkedIn data for: {company_name}")

    try:
        sheet = get_sheet()
        clear_and_write(sheet, company_name)

        # Launch all Phantoms
        launch_agent(PHANTOMS["company_url_finder"], {"argument": {"spreadsheetUrl": SHEET_URL}})
        launch_agent(PHANTOMS["employee_export"])
        launch_agent(PHANTOMS["activity_explorer"])

        # Start static 2‑minute countdown immediately
        log.info("⏳ Started 2‑minute timer…")
        time.sleep(120)

        # Download, filter, analyze
        result_path = download_and_filter(company_name)
        summary = analyze_with_gemini(result_path, company_name)

        # Clean up
        delete_all_outputs()

        # Convert to standardized format
        linkedin_data = [{
            'headline': f"LinkedIn Activity Summary for {company_name}",
            'description': str(summary)[:200] + '...' if len(str(summary)) > 200 else str(summary),
            'url': SHEET_URL,
            'image_url': '',
            'full_content': str(summary),
            'source': 'linkedin',
            'date': time.strftime('%Y-%m-%d')
        }]

        # Save LinkedIn data to JSON file
        filename = f"linkedin_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(linkedin_data, f, indent=2, ensure_ascii=False)
        print(f"💾 LinkedIn data saved to {filename}")

        print(f"✅ Found LinkedIn data for {company_name}")
        return linkedin_data

    except Exception as e:
        log.error(f"❌ LinkedIn scraping failed for {company_name}: {e}")
        return []

# --- Main Workflow (for standalone testing) ---
if __name__ == "__main__":
    company = input("Enter company name: ").strip()
    result = get_company_linkedin_data(company)
    print("\n🧠 LinkedIn Summary:")
    for item in result:
        print(f"- {item['headline']}")
        print(f"  {item['full_content']}")
