import os
from pathlib import Path

import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]
_ROOT = Path(__file__).resolve().parents[2]
_cred = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip() or str(_ROOT / "credentials.json")
creds = Credentials.from_service_account_file(_cred, scopes=scopes)
client = gspread.authorize(creds)
try:
    import config as _cfg

    _cfg_sid = (getattr(_cfg, "SK_TOOLS_SPREADSHEET_ID", None) or "").strip()
except Exception:
    _cfg_sid = ""
sheet_id = (
    _cfg_sid
    or os.getenv("SK_TOOLS_SPREADSHEET_ID")
    or "176wSQDDz9D1APmAXiYPeECwMqCQm3mvMBwgj8MKqmgk"
).strip()
workbook = client.open_by_key(sheet_id)

def create_or_update_sheet_from_list(sheet_name, data):
    spreadsheet = workbook
    """
    Create a new worksheet if it doesn't exist, or clear and update it if it does.

    Args:
        spreadsheet (gspread.Spreadsheet): A gspread Spreadsheet object.
        sheet_name (str): The name of the worksheet to create or update.
        data (List[List[Any]]): A 2D list of values to write into the sheet.
    """
    try:
        # Try to get the worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Clearing and updating...")
        worksheet.clear()
    except WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        rows = max(len(data), 1)
        cols = max(len(data[0]) if data else 1, 1)
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

    # Update the sheet with new data
    if data:
        cell_range = f"A1"
        worksheet.update(cell_range, data)
    print(f"Worksheet '{sheet_name}' updated successfully.")

def create_or_update_sheet_from_dicts(sheet_name, dict_data):
    spreadsheet = workbook
    if not dict_data:
        print("No data provided.")
        return

    headers = list(dict_data[0].keys())

    # Efficient sanitization: minimal checks
    def sanitize_row(row):
        return [row[h] if isinstance(row[h], (int, float, str)) or row[h] is None else str(row[h]) for h in headers]

    rows = [sanitize_row(row) for row in dict_data]
    data = [headers] + rows

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Clearing and updating...")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    worksheet.update(data)
    print(f"Worksheet '{sheet_name}' updated successfully.")

def read_sheet(sheet_name):
    sheet = workbook.worksheet(sheet_name)
    values = sheet.get_all_values()

    if not values:
        return []

    headers = values[0]
    rows = values[1:]

    # Convert each row into a dictionary, padding missing values
    data = [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows]

    return data


def read_sheet_withID(sheet_id,sheet_name):
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    values = sheet.get_all_values()

    if not values:
        return []

    headers = values[0]
    rows = values[1:]

    # Convert each row into a dictionary, padding missing values
    data = [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in rows]

    return data
    
def create_or_update_sheet_from_list_withId(sheetId, sheet_name, data):
    spreadsheet =  client.open_by_key(sheetId)
    """
    Create a new worksheet if it doesn't exist, or clear and update it if it does.

    Args:
        spreadsheet (gspread.Spreadsheet): A gspread Spreadsheet object.
        sheet_name (str): The name of the worksheet to create or update.
        data (List[List[Any]]): A 2D list of values to write into the sheet.
    """
    try:
        # Try to get the worksheet
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Clearing and updating...")
        worksheet.clear()
    except WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        rows = max(len(data), 1)
        cols = max(len(data[0]) if data else 1, 1)
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

    # Update the sheet with new data
    if data:
        cell_range = f"A1"
        worksheet.update(cell_range, data)
    print(f"Worksheet '{sheet_name}' updated successfully.")

def create_or_update_sheet_from_dicts_withId(sheetId,sheet_name, dict_data):
    spreadsheet =  client.open_by_key(sheetId)
    if not dict_data:
        print("No data provided.")
        return

    headers = list(dict_data[0].keys())

    # Efficient sanitization: minimal checks
    def sanitize_row(row):
        return [row[h] if isinstance(row[h], (int, float, str)) or row[h] is None else str(row[h]) for h in headers]

    rows = [sanitize_row(row) for row in dict_data]
    data = [headers] + rows

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Clearing and updating...")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    worksheet.update(data)
    print(f"Worksheet '{sheet_name}' updated successfully.")

def create_or_update_sheet_from_dicts_withID(sheet_id, sheet_name, dict_data):
    spreadsheet = client.open_by_key(sheet_id)
    if not dict_data:
        print("No data provided.")
        return

    headers = list(dict_data[0].keys())

    # Efficient sanitization: minimal checks
    def sanitize_row(row):
        return [row[h] if isinstance(row[h], (int, float, str)) or row[h] is None else str(row[h]) for h in headers]

    rows = [sanitize_row(row) for row in dict_data]
    data = [headers] + rows

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Clearing and updating...")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    worksheet.update(data)
    print(f"Worksheet '{sheet_name}' updated successfully.")