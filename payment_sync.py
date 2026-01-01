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

# Get all existing rows from sheet (to check what's already logged)
existing_rows = sheet.get_all_records()
existing_payments = set()

# Build a set of existing payments (client + payment type + date)
for row in existing_rows:
    key = f"{row.get('Client Name', '')}|{row.get('Payment Type', '')}|{row.get('Payment Date', '')}"
    existing_payments.add(key)

# If sheet is empty, add headers
if len(existing_rows) == 0:
    sheet.append_row(['Client Name', 'Client Email', 'Closer', 'Setter', 'Payment Type', 'Payment Date', 'Payment Amount', 'Status', 'Date Added'])

# Get all records from Airtable
records = table.all()

# Track what we added this run
added_count = 0

# Process each record
for record in records:
    fields = record['fields']
    client_name = fields.get('Client Name', '')
    client_email = fields.get('Client Email', '')
    closer = fields.get('Closer', '')
    setter = fields.get('Setter', '')
    
    # Skip if no client info
    if not client_name and not client_email:
        continue
    
    # Check 2nd payment
    payment_2_date_str = fields.get('Date of 2nd Payment', '')
    payment_2_amount = fields.get('Amount Due For 2nd Payment', '')
    
    if payment_2_date_str and payment_2_amount:
        payment_2_date = datetime.fromisoformat(payment_2_date_str).date()
        
        # Create unique key for this payment
        payment_key = f"{client_name}|2nd Payment|{payment_2_date_str}"
        
        # Only add if payment date has arrived AND it's not already in the sheet
        if payment_2_date <= today and payment_key not in existing_payments:
            # Determine status
            if payment_2_date < today:
                status = '🚨 OVERDUE'
            elif payment_2_date == today:
                status = '⏰ DUE TODAY'
            else:
                status = '✅ LOGGED'
            
            # Add to sheet with today's date
            sheet.append_row([
                client_name, 
                client_email, 
                closer, 
                setter, 
                '2nd Payment', 
                payment_2_date_str, 
                payment_2_amount, 
                status,
                str(today)  # Date it was added to the log
            ])
            added_count += 1
            print(f"✅ Added: {client_name} - 2nd Payment (£{payment_2_amount})")
    
    # Check 3rd payment
    payment_3_date_str = fields.get('Date of 3rd Payment', '')
    payment_3_amount = fields.get('Amount Due For 3rd Payment', '')
    
    if payment_3_date_str and payment_3_amount:
        payment_3_date = datetime.fromisoformat(payment_3_date_str).date()
        payment_key = f"{client_name}|3rd Payment|{payment_3_date_str}"
        
        if payment_3_date <= today and payment_key not in existing_payments:
            if payment_3_date < today:
                status = '🚨 OVERDUE'
            elif payment_3_date == today:
                status = '⏰ DUE TODAY'
            else:
                status = '✅ LOGGED'
            
            sheet.append_row([
                client_name, 
                client_email, 
                closer, 
                setter, 
                '3rd Payment', 
                payment_3_date_str, 
                payment_3_amount, 
                status,
                str(today)
            ])
            added_count += 1
            print(f"✅ Added: {client_name} - 3rd Payment (£{payment_3_amount})")
    
    # Check 4th payment
    payment_4_date_str = fields.get('Date of 4th Payment', '')
    payment_4_amount = fields.get('Amount due for 4th Payment', '')  # Note: lowercase 'd' in 'due'
    
    if payment_4_date_str and payment_4_amount:
        payment_4_date = datetime.fromisoformat(payment_4_date_str).date()
        payment_key = f"{client_name}|4th Payment|{payment_4_date_str}"
        
        if payment_4_date <= today and payment_key not in existing_payments:
            if payment_4_date < today:
                status = '🚨 OVERDUE'
            elif payment_4_date == today:
                status = '⏰ DUE TODAY'
            else:
                status = '✅ LOGGED'
            
            sheet.append_row([
                client_name, 
                client_email, 
                closer, 
                setter, 
                '4th Payment', 
                payment_4_date_str, 
                payment_4_amount, 
                status,
                str(today)
            ])
            added_count += 1
            print(f"✅ Added: {client_name} - 4th Payment (£{payment_4_amount})")

# Final summary
if added_count > 0:
    print(f"\n✅ Payment sync complete - {added_count} new payment(s) added")
else:
    print("\n✅ Payment sync complete - no new payments due today")
