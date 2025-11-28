from pyairtable import Api
import gspread
from google.oauth2.service_account import Credentials
import os
import json
from datetime import date

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
today = date.today().isoformat()

# Get all records from Airtable
records = table.all()

# Get all existing rows (skip header)
existing_rows = sheet.get_all_values()[1:]  # Skip header row

# Process each record
for record in records:
    fields = record['fields']
    client_name = fields.get('Client Name', '')
    client_email = fields.get('Client Email', '')
    
    # Skip if no client info
    if not client_name and not client_email:
        continue
    
    # Find existing row for this client
    row_index = None
    for idx, row in enumerate(existing_rows):
        if len(row) >= 2 and (row[0] == client_name or row[1] == client_email):
            row_index = idx + 2  # +2 for header row and 0-indexing
            break
    
    # Check if 2nd payment is due today
    payment_2_date = fields.get('Date of 2nd Payment', '')
    payment_2_amount = fields.get('Amount Due For 2nd Payment', '')
    
    # Check if 3rd payment is due today
    payment_3_date = fields.get('Date of 3rd Payment', '')
    payment_3_amount = fields.get('Amount Due For 3rd Payment', '')
    
    # If 2nd payment is due today
    if payment_2_date == today and payment_2_amount:
        if row_index:
            # Update existing row - columns C and D
            sheet.update_cell(row_index, 3, payment_2_amount)
            sheet.update_cell(row_index, 4, payment_2_date)
        else:
            # Create new row
            sheet.append_row([client_name, client_email, payment_2_amount, payment_2_date, '', ''])
            existing_rows.append([client_name, client_email, payment_2_amount, payment_2_date, '', ''])
            row_index = len(existing_rows) + 1
    
    # If 3rd payment is due today
    if payment_3_date == today and payment_3_amount:
        if row_index:
            # Update existing row - columns E and F
            sheet.update_cell(row_index, 5, payment_3_amount)
            sheet.update_cell(row_index, 6, payment_3_date)
        else:
            # Create new row (shouldn't happen but just in case)
            sheet.append_row([client_name, client_email, '', '', payment_3_amount, payment_3_date])

print('Payment sync complete')
