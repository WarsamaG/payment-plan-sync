from pyairtable import Api
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import datetime, date
# Get credentials from environment
AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
GOOGLE_CREDENTIALS = json.loads(os.environ['GOOGLE_CREDENTIALS'])
# Airtable setup
BASE_ID = 'appr1ojQ7J6ZkSI3z'  # Your DK Sales Data base ID
TABLE_ID = 'tblSiOUNtCuZCnIFf'  # Your Payment Plan table ID
api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_ID)
# Google Sheets setup
scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds = Credentials.from_service_account_info(GOOGLE_CREDENTIALS, scopes=scope)
client = gspread.authorize(creds)
sheet = client.open('Payment Plans').sheet1
# Get today's date
today = date.today()
# Get all records from Airtable
records = table.all()
# Clear existing data (keep header)
sheet.clear()
sheet.append_row(['Client Name', 'Client Email', 'Closed By', 'Set By', 'Payment Type', 'Payment Date', 'Payment Amount'])
# Process each record
for record in records:
    fields = record['fields']
    client_name = fields.get('Client Name', '')
    client_email = fields.get('Client Email', '')
    
    # Get closer and setter - handle if they're lists (lookup fields return lists)
    closer_raw = fields.get('Closer', '')
    setter_raw = fields.get('Setter', '')
    
    # Convert to string if it's a list
    closer = closer_raw[0] if isinstance(closer_raw, list) and len(closer_raw) > 0 else closer_raw
    setter = setter_raw[0] if isinstance(setter_raw, list) and len(setter_raw) > 0 else setter_raw
    
    # Skip if no client info
    if not client_name and not client_email:
        continue
    
    # Check 2nd payment
    payment_2_date_str = fields.get('Date of 2nd Payment', '')
    payment_2_amount = fields.get('Amount Due For 2nd Payment', '')
    
    if payment_2_date_str and payment_2_amount:
        payment_2_date = datetime.fromisoformat(payment_2_date_str).date()
        if payment_2_date <= today:
            sheet.append_row([client_name, client_email, closer, setter, '2nd Payment', payment_2_date_str, payment_2_amount])
    
    # Check 3rd payment
    payment_3_date_str = fields.get('Date of 3rd Payment', '')
    payment_3_amount = fields.get('Amount Due For 3rd Payment', '')
    
    if payment_3_date_str and payment_3_amount:
        payment_3_date = datetime.fromisoformat(payment_3_date_str).date()
        if payment_3_date <= today:
            sheet.append_row([client_name, client_email, closer, setter, '3rd Payment', payment_3_date_str, payment_3_amount])
    
    # Check 4th payment
    payment_4_date_str = fields.get('Date of 4th Payment', '')
    payment_4_amount = fields.get('Amount due for 4th Payment', '')
    
    if payment_4_date_str and payment_4_amount:
        payment_4_date = datetime.fromisoformat(payment_4_date_str).date()
        if payment_4_date <= today:
            sheet.append_row([client_name, client_email, closer, setter, '4th Payment', payment_4_date_str, payment_4_amount])
print('Payment sync complete')
