import os
import json
import base64
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Set Gmail read-only scope
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def authenticate_gmail(user_tag):
    """Authenticate and cache token for a specific Gmail user (identified by user_tag)."""
    creds = None
    token_file = f'token_{user_tag}.json'

    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)  # First-time only: OAuth login for that Gmail
        with open(token_file, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def search_messages(service, query):
    """Search Gmail for messages matching the query."""
    results = service.users().messages().list(userId='me', q=query).execute()
    messages = results.get('messages', [])
    return messages

def get_message_details(service, msg_id):
    """Extract sender, subject, date, and plain body from a message ID."""
    msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
    headers = msg['payload'].get('headers', [])
    subject = sender = date = "N/A"
    for h in headers:
        if h['name'] == 'Subject':
            subject = h['value']
        elif h['name'] == 'From':
            sender = h['value']
        elif h['name'] == 'Date':
            date = h['value']

    # Try to find HTML or plain body
    parts = msg['payload'].get('parts', [])
    body = ""
    if parts:
        for part in parts:
            if part['mimeType'] in ['text/html', 'text/plain']:
                data = part['body'].get('data')
                if data:
                    decoded_data = base64.urlsafe_b64decode(data).decode('utf-8')
                    soup = BeautifulSoup(decoded_data, 'html.parser')
                    body = soup.get_text()
                    break
    else:
        data = msg['payload'].get('body', {}).get('data')
        if data:
            decoded_data = base64.urlsafe_b64decode(data).decode('utf-8')
            soup = BeautifulSoup(decoded_data, 'html.parser')
            body = soup.get_text()

    return {
        'subject': subject,
        'sender': sender,
        'date': date,
        'body': body[:1000]  # Limit for preview
    }

def extract_emails_for_company(user_tag, company_name):
    """Extract emails for a company and return them in a structured format."""
    service = authenticate_gmail(user_tag)
    query = f'"{company_name}"'
    messages = search_messages(service, query)
    print(f"\n🔎 Found {len(messages)} emails related to: {company_name}")

    email_data = []
    for msg in messages:
        msg_id = msg['id']
        details = get_message_details(service, msg_id)
        
        # Convert to standardized format
        email_data.append({
            'headline': details['subject'],
            'description': details['body'][:200] + '...' if len(details['body']) > 200 else details['body'],
            'url': f"https://mail.google.com/mail/u/0/#inbox/{msg_id}",
            'image_url': '',
            'full_content': details['body'],
            'source': 'gmail',
            'date': details['date'],
            'sender': details['sender']
        })
    
    return email_data

def get_company_gmail_data(company_name: str):
    """
    Main function to get Gmail data for a company
    Returns standardized data format compatible with the main workflow
    """
    print(f"[GMAIL] Processing Gmail data for: {company_name}")

    try:
        user_tag = "portfolio@elevationai.com"
        gmail_data = extract_emails_for_company(user_tag, company_name)

        # Save Gmail data to JSON file
        filename = f"gmail_{company_name.replace(' ', '_').replace('.', '').lower()}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(gmail_data, f, indent=2, ensure_ascii=False)
        print(f"[SAVE] Gmail data saved to {filename}")

        print(f"[SUCCESS] Found {len(gmail_data)} Gmail messages for {company_name}")
        return gmail_data

    except Exception as e:
        print(f"[ERROR] Gmail scraping failed for {company_name}: {e}")
        return []

# ========= ENTRY POINT =========
if __name__ == "__main__":
    user_tag = "portfolio@elevationai.com"
    company = input("Enter company name to search emails for: ").strip()
    result = get_company_gmail_data(company)
    
    print(f"\n[GMAIL] Gmail Summary for {company}:")
    for email in result:
        print("=" * 80)
        print(f"[FROM] From: {email['sender']}")
        print(f"[SUBJECT] Subject: {email['headline']}")
        print(f"[DATE] Date: {email['date']}")
        print(f"[BODY] Body: {email['description']}")
        print("=" * 80)
