import os
import re
import pandas as pd
import dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

dotenv.load_dotenv()

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
    
    # pdf_path = 'SAUMAC_PRESSKIT.pdf'
    # if os.path.exists(pdf_path):
    #     with open(pdf_path, 'rb') as attachment:
    #         part = MIMEBase('application', 'octet-stream')
    #         part.set_payload(attachment.read())
        
    #     encoders.encode_base64(part)
    #     part.add_header(
    #         'Content-Disposition',
    #         f'attachment; filename= {os.path.basename(pdf_path)}'
    #     )
    #     msg.attach(part)
    #     print(f"- Attached PDF: {os.path.basename(pdf_path)}")
    # elif pdf_path:
    #     print(f"- Warning: PDF file not found: {pdf_path}")
    
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
    
    # if sent:
        # df.loc[i, 'Contacted'] = 'Yes'
        # df.to_csv('data.csv', index=False)

