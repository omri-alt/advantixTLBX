import os
import time
from pathlib import Path

import gspread
from gspread.exceptions import WorksheetNotFound
from gspread.utils import rowcol_to_a1
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


def _is_transient_sheets_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        x in msg
        for x in (
            "connection reset",
            "connection aborted",
            "remote end closed",
            "timed out",
            "timeout",
            "503",
            "502",
            "429",
            "quota",
        )
    )


def _pad_grid(data) -> list[list]:
    if not data:
        return []
    width = max((len(r) for r in data), default=1)
    out: list[list] = []
    for row in data:
        cells = list(row) if row is not None else []
        if len(cells) < width:
            cells = cells + [""] * (width - len(cells))
        elif len(cells) > width:
            cells = cells[:width]
        out.append(cells)
    return out


def _replace_worksheet_values(worksheet, data, *, attempts: int = 4) -> None:
    """Write a grid from A1 without clearing first, then drop leftover cells.

    Avoids the empty-flash race of ``clear()`` + ``update()`` and skips the
    leftover clear when the sheet is already the same size.
    """
    grid = _pad_grid(data)
    last_err: Exception | None = None
    for attempt in range(attempts):
        try:
            if not grid:
                worksheet.clear()
                return
            rows = len(grid)
            cols = len(grid[0])
            rng = f"A1:{rowcol_to_a1(rows, cols)}"
            worksheet.update(rng, grid, value_input_option="USER_ENTERED")
            leftover: list[str] = []
            try:
                row_count = int(getattr(worksheet, "row_count", 0) or 0)
                col_count = int(getattr(worksheet, "col_count", 0) or 0)
            except (TypeError, ValueError):
                row_count, col_count = 0, 0
            if row_count > rows:
                leftover.append(
                    f"A{rows + 1}:{rowcol_to_a1(row_count, max(col_count, cols))}"
                )
            if col_count > cols:
                leftover.append(
                    f"{rowcol_to_a1(1, cols + 1)}:{rowcol_to_a1(min(rows, row_count) or rows, col_count)}"
                )
            if leftover:
                worksheet.batch_clear(leftover)
            return
        except Exception as e:
            last_err = e
            if not _is_transient_sheets_error(e) or attempt >= attempts - 1:
                raise
            time.sleep(min(60, 5 * (attempt + 1)))
    if last_err:
        raise last_err


def _worksheet_update_with_retry(worksheet, data, *, attempts: int = 4) -> None:
    """Retry Google Sheets writes on transient connection resets."""
    _replace_worksheet_values(worksheet, data, attempts=attempts)

def create_or_update_sheet_from_list(sheet_name, data):
    spreadsheet = workbook
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Updating...")
    except WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        rows = max(len(data), 1)
        cols = max(len(data[0]) if data else 1, 1)
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

    _replace_worksheet_values(worksheet, data or [])
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
        print(f"Worksheet '{sheet_name}' exists. Updating...")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    _replace_worksheet_values(worksheet, data)
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
    spreadsheet = client.open_by_key(sheetId)
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"Worksheet '{sheet_name}' exists. Updating...")
    except WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        rows = max(len(data), 1)
        cols = max(len(data[0]) if data else 1, 1)
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=str(rows), cols=str(cols))

    _replace_worksheet_values(worksheet, data or [])
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
        print(f"Worksheet '{sheet_name}' exists. Updating...")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    _replace_worksheet_values(worksheet, data)
    print(f"Worksheet '{sheet_name}' updated successfully.")

def append_missing_headers_row1(
    spreadsheet_id: str,
    worksheet_title: str,
    ordered_headers: list,
    *,
    create_if_missing: bool = True,
) -> list[str]:
    """
    Ensure a worksheet exists with the required header names in row 1.

    - Missing worksheet: if ``create_if_missing`` (default), create it and set row 1 to
      ``ordered_headers``; otherwise raise ``WorksheetNotFound``.
    - Empty row 1: replaced with ``ordered_headers``.
    - Non-empty row 1: any name in ``ordered_headers`` not already present is **appended**
      (preserves existing column order and data).

    Returns the list of header names that were newly appended (empty if none).
    """
    spreadsheet = client.open_by_key(spreadsheet_id)
    try:
        ws = spreadsheet.worksheet(worksheet_title)
    except WorksheetNotFound:
        if not create_if_missing:
            raise
        cols = max(len(ordered_headers), 1)
        ws = spreadsheet.add_worksheet(title=worksheet_title, rows="2000", cols=str(cols))
        ws.update("A1", [ordered_headers], value_input_option="USER_ENTERED")
        return list(ordered_headers)

    row1 = ws.row_values(1)
    while row1 and str(row1[-1]).strip() == "":
        row1.pop()
    def _col_count(w) -> int:
        try:
            return int(getattr(w, "col_count", 0) or 0)
        except (TypeError, ValueError):
            return 0

    if not row1 or not any(str(x).strip() for x in row1):
        cc = max(_col_count(ws), len(ordered_headers))
        ws.resize(rows=max(ws.row_count, 1000), cols=cc)
        rng = f"A1:{rowcol_to_a1(1, len(ordered_headers))}"
        ws.update(rng, [ordered_headers], value_input_option="USER_ENTERED")
        return list(ordered_headers)

    seen = set(row1)
    merged = list(row1)
    added: list[str] = []
    for h in ordered_headers:
        if h not in seen:
            merged.append(h)
            seen.add(h)
            added.append(h)
    if not added:
        return []
    n = len(merged)
    ws.resize(rows=max(ws.row_count, 1000), cols=max(n, _col_count(ws)))
    rng = f"A1:{rowcol_to_a1(1, n)}"
    ws.update(rng, [merged], value_input_option="USER_ENTERED")
    return added


def ensure_worksheet_with_headers(spreadsheet_id: str, worksheet_title: str, headers: list) -> None:
    """
    If ``worksheet_title`` is missing, create it. If row 1 is empty, write ``headers``.
    Does not clear existing data when headers are already present.
    """
    spreadsheet = client.open_by_key(spreadsheet_id)
    try:
        ws = spreadsheet.worksheet(worksheet_title)
    except WorksheetNotFound:
        cols = max(len(headers), 1)
        ws = spreadsheet.add_worksheet(title=worksheet_title, rows="2000", cols=str(cols))
    row1 = ws.row_values(1)
    if not row1 or not any(str(x).strip() for x in row1):
        ws.update("A1", [headers], value_input_option="USER_ENTERED")


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
        print(f"Worksheet '{sheet_name}' exists. Updating...")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it...")
        worksheet = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=str(len(data)),
            cols=str(len(headers))
        )

    _replace_worksheet_values(worksheet, data)
    print(f"Worksheet '{sheet_name}' updated successfully.")