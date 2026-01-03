import os
import re
import pandas as pd
import dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import gspread
from google.oauth2.service_account import Credentials

dotenv.load_dotenv()

def get_google_sheets_client(spreadsheet_id=None, worksheet_name='Sheet1', credentials_path=None):
    """
    Helper function to get authenticated Google Sheets client and worksheet.
    Returns (client, spreadsheet, worksheet) tuple or (None, None, None) if failed.
    """
    # Get worksheet name from environment if not provided
    if worksheet_name == 'Sheet1':
        env_worksheet = os.getenv('GOOGLE_SHEETS_WORKSHEET')
        if env_worksheet:
            worksheet_name = env_worksheet
    
    # Get credentials path from environment or parameter
    if credentials_path is None:
        credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
        if credentials_path is None:
            return None, None, None, "GOOGLE_CREDENTIALS_PATH not set in .env file"
    
    # Get spreadsheet ID from environment or parameter
    if spreadsheet_id is None:
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
        if spreadsheet_id is None:
            return None, None, None, "GOOGLE_SHEETS_ID not set in .env file"
    
    # Check if credentials file exists
    if not os.path.exists(credentials_path):
        return None, None, None, f"Credentials file not found at: {credentials_path}"
    
    try:
        import json
        
        if not credentials_path.lower().endswith('.json'):
            return None, None, None, f"Credentials at {credentials_path} is not a .json file."
        
        # Try to unpack the JSON and check if it's readable
        try:
            with open(credentials_path, "r") as f:
                creds_json = json.load(f)
        except Exception as e:
            return None, None, None, f"Could not read credentials JSON: {e}"

        # Authenticate with Google Sheets API
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        return client, spreadsheet, worksheet, None
    except gspread.exceptions.SpreadsheetNotFound:
        return None, None, None, f"Spreadsheet with ID '{spreadsheet_id}' not found. Check the ID and ensure the service account has access."
    except gspread.exceptions.WorksheetNotFound as e:
        try:
            available_worksheets = [ws.title for ws in spreadsheet.worksheets()]
            return None, None, None, f"Worksheet '{worksheet_name}' not found. Available worksheets: {', '.join(available_worksheets)}"
        except:
            return None, None, None, f"Worksheet '{worksheet_name}' not found in the spreadsheet."
    except Exception as e:
        return None, None, None, f"Authentication error: {str(e)}"

def get_data_from_google_sheets(spreadsheet_id=None, worksheet_name='Sheet1', credentials_path=None):
    """
    Fetch data from Google Sheets and return it as a pandas DataFrame.
    
    Args:
        spreadsheet_id: The Google Sheets spreadsheet ID (from the URL)
                        If None, will try to get from GOOGLE_SHEETS_ID env variable
        worksheet_name: Name of the worksheet/tab to read from (default: 'Sheet1')
        credentials_path: Path to the service account JSON credentials file
                         If None, will try to get from GOOGLE_CREDENTIALS_PATH env variable
    
    Returns:
        pandas.DataFrame: The data from the Google Sheet
    
    Example:
        df = get_data_from_google_sheets(
            spreadsheet_id='1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms',
            worksheet_name='Sheet1',
            credentials_path='credentials.json'
        )
    """
    client, spreadsheet, worksheet, error_msg = get_google_sheets_client(spreadsheet_id, worksheet_name, credentials_path)
    
    if worksheet is None:
        raise ValueError(
            f"Unable to connect to Google Sheets.\n{error_msg}\n\n"
            "Please check:\n"
            "1. GOOGLE_CREDENTIALS_PATH is set in .env file\n"
            "2. GOOGLE_SHEETS_ID is set in .env file\n"
            "3. Credentials file exists and is valid\n"
            "4. Service account has access to the spreadsheet"
        )
    
    try:
        # Get all values
        data = worksheet.get_all_values()
        
        # Convert to DataFrame (first row as headers)
        if len(data) > 0:
            df = pd.DataFrame(data[1:], columns=data[0])
            print(f"✓ Successfully fetched {len(df)} rows from Google Sheets")
            return df
        else:
            print("⚠ Warning: Google Sheet appears to be empty")
            return pd.DataFrame()
            
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"Spreadsheet with ID '{spreadsheet_id}' not found. Check the ID and ensure the service account has access.")
    except gspread.exceptions.WorksheetNotFound:
        raise ValueError(f"Worksheet '{worksheet_name}' not found in the spreadsheet.")
    except Exception as e:
        raise Exception(f"Error fetching data from Google Sheets: {str(e)}")

def update_google_sheets_row(row_index, column_name, value, spreadsheet_id=None, worksheet_name='Sheet1', credentials_path=None):
    """
    Update a specific cell in Google Sheets.
    
    Args:
        row_index: The row index (0-based, excluding header)
        column_name: The column name (header)
        value: The new value to set
        spreadsheet_id: The Google Sheets spreadsheet ID
        worksheet_name: Name of the worksheet/tab
        credentials_path: Path to the service account JSON credentials file
    """
    client, spreadsheet, worksheet, error_msg = get_google_sheets_client(spreadsheet_id, worksheet_name, credentials_path)
    
    if worksheet is None:
        if error_msg:
            print(f"⚠ Warning: {error_msg}")
        return False
    
    try:
        # Find the column index
        headers = worksheet.row_values(1)
        if column_name not in headers:
            print(f"⚠ Warning: Column '{column_name}' not found in Google Sheet")
            return False
        
        col_index = headers.index(column_name) + 1  # gspread uses 1-based indexing
        row_num = row_index + 2  # +1 for header, +1 for 1-based indexing
        
        # Update the cell
        worksheet.update_cell(row_num, col_index, value)
        return True
    except Exception as e:
        print(f"⚠ Warning: Failed to update Google Sheets: {e}")
        return False

# Fetch data from Google Sheets
# Store whether we're using Google Sheets for later updates
using_google_sheets = False
try:
    df = get_data_from_google_sheets()
    using_google_sheets = True
except Exception as e:
    print(f"Error loading from Google Sheets: {e}")
    print("Falling back to local data.csv file...")
    df = pd.read_csv('data.csv')

def check_row(row):
    if row['Channel'] not in ['email', 'instagram', 'soundcloud']:
        print(f"Invalid channel: {row['Channel']}")
        return False
    if row['Channel'] == 'email':
        if not re.match(r"[^@]+@[^@]+\.[^@]+", row['Handle']):
            print(f"Invalid email: {row['Handle']}")
            return False
    if row['Channel'] == 'instagram':
        if not re.match(r"^[a-zA-Z0-9_.]+$", row['Handle']):
            print(f"Invalid Instagram handle: {row['Handle']}")
            return False
    if row['Channel'] == 'soundcloud':
        print(f"Soundcloud not supported yet")
        return False
        if not re.match(r"^[a-zA-Z0-9_.]+$", row['Handle']):
            print(f"Invalid SoundCloud handle: {row['Handle']}")
            return False
    if row['Name'] == '':
        print(f"Empty Name for {row['Handle']}")
        return False
    if row['Event_Name'] == '':
        print(f"Empty Event Name for {row['Handle']}")
        return False
    if row['Language'] not in ['EN', 'FR']:
        print(f"Invalid language: {row['Language']}")
        return False
    message_file_name = f"{row['Message_Type']}_{row['Language']}.txt"
    if not os.path.exists(message_file_name):
        print(f"Message file not found: {message_file_name}")
        return False
    if row['Contacted'] not in ['Yes', 'No']:
        print(f"Invalid contacted: {row['Contacted']}")
        return False
    if row['Contacted'] == 'Yes':
        print(f"Already contacted")
        return False
    return True

def send_email(handle, message: str, subject: str):  
    if not message or not subject:
        print(f"Message or subject is empty!")
        return False

    sender_email = os.getenv('SENDER_EMAIL')
    sender_password = os.getenv('SENDER_PASSWORD')
    receiver_email = handle
    
    msg = MIMEMultipart()
    msg['From'] = f'Saumac Music <{sender_email}>'
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    # Convert message to HTML with clickable links
    html_message = convert_text_to_html(message)
    msg.attach(MIMEText(html_message, 'html'))
    
    pdf_path = 'SAUMAC_PRESSKIT.pdf'
    if os.path.exists(pdf_path):
        with open(pdf_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {os.path.basename(pdf_path)}'
        )
        msg.attach(part)
        print(f"- Attached PDF: {os.path.basename(pdf_path)}")
    elif pdf_path:
        print(f"- Warning: PDF file not found: {pdf_path}")
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print(f"- Sent by email to {handle}")
        return True
    except Exception as e:
        print(f"-X- Failed to send email: {e}")
        return False

def send_instagram(handle, message):
    print(f"-X- Instagram not supported yet")
    return False
    print(f"- Sent by Instagram to {handle}")
    return False

def convert_text_to_html(text: str) -> str:
    """
    Convert plain text to HTML, preserving line breaks and converting link syntax.
    Link syntax: [Link Text](https://url.com)
    """
    # Convert link syntax [text](url) to HTML anchor tags
    link_pattern = r'\[([^\]]+)\]\(([^\)]+)\)'
    html = re.sub(link_pattern, r'<a href="\2">\1</a>', text)
    
    # Convert line breaks to HTML <br> tags
    html = html.replace('\n', '<br>\n')
    
    # Wrap in basic HTML structure for better email client compatibility
    html = f'<html><body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">{html}</body></html>'
    
    return html

def generate_message(row):
    message_file_name = f"{row['Message_Type']}_{row['Language']}.txt"
    with open(message_file_name, 'r') as f:
        all_text = f.read()

    subject, message = all_text.split('}\n\n')
    subject = subject.replace('{Subject: ', '')
    subject = subject.replace('[Name]', row['Name'])
    subject = subject.replace('[Event Name]', row['Event_Name'])
    message = message.replace('[Name]', row['Name'])
    message = message.replace('[Event Name]', row['Event_Name'])
    return message, subject
            
for i in range(len(df)):
    print(f"\nWriting to {df.iloc[i]['Name']}:")
    valid = check_row(df.iloc[i])
    if not valid:
        continue

    message, subject = generate_message(df.iloc[i])
    print(f"- Generated {df.iloc[i]['Message_Type']}_{df.iloc[i]['Language']} message")

    sent = False
    if df.iloc[i]['Channel'] == 'email':
        sent = send_email(df.iloc[i]['Handle'], message, subject)
    elif df.iloc[i]['Channel'] == 'instagram':
        sent = send_instagram(df.iloc[i]['Handle'], message)
    
    if sent:
        df.loc[i, 'Contacted'] = 'Yes'
        # Update Google Sheets if we're using it, otherwise save to CSV
        if using_google_sheets:
            update_google_sheets_row(i, 'Contacted', 'Yes')
        else:
            df.to_csv('data.csv', index=False)

