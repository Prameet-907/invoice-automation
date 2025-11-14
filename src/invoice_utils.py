# src/invoice_utils.py
import os, re, time, json
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date, timedelta
from functools import wraps
from googleapiclient.errors import HttpError

# ---------- utils ----------
def retry(max_attempts=3, backoff=1.0):
    def deco(f):
        @wraps(f)
        def wrapper(*a, **kw):
            attempt = 0
            while True:
                try:
                    return f(*a, **kw)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    time.sleep(backoff * (2 ** (attempt - 1)))
        return wrapper
    return deco

def round2_decimal(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def normalize_text(s: Any) -> str:
    if s is None:
        return ""
    t = str(s)
    t = t.strip()
    # NFKC not required; collapse whitespace & lowercase
    t = re.sub(r'\s+', ' ', t)
    return t.lower()

def parse_amount(raw: Any) -> Decimal:
    s = "" if raw is None else str(raw)
    cleaned = re.sub(r'[^0-9.\-]', '', s)
    if cleaned in ("", ".", "-"):
        return Decimal("0")
    try:
        return Decimal(cleaned)
    except:
        return Decimal("0")

def parse_month_string(s: str) -> Optional[Dict[str,Any]]:
    if not s:
        return None
    s = str(s).strip()
    months = [
        "January","February","March","April","May","June","July","August","September","October","November","December"
    ]
    m = re.match(r'^([A-Za-z]+)\s+(\d{4})$', s)
    if m:
        mon = m.group(1)
        yr = int(m.group(2))
        for idx,mn in enumerate(months):
            if mn.lower().startswith(mon[:3].lower()):
                # last day:
                if idx == 11:
                    last_day = 31
                else:
                    next_month = date(yr, idx+2, 1)
                    last_day = (next_month - timedelta(days=1)).day
                return {"year":yr,"monthName":months[idx],"lastDay":last_day,"lastDate":date(yr, idx+1, last_day)}
    m2 = re.match(r'^(\d{1,2})[\/\-](\d{4})$', s)
    if m2:
        midx = int(m2.group(1))-1
        if 0 <= midx <= 11:
            yr = int(m2.group(2))
            if midx == 11:
                last_day = 31
            else:
                next_month = date(yr, midx+2, 1)
                last_day = (next_month - timedelta(days=1)).day
            return {"year":yr,"monthName":months[midx],"lastDay":last_day,"lastDate":date(yr, midx+1, last_day)}
    return None

# ---------- Sheets helpers (assumes googleapiclient sheets.spreadsheets() client passed) ----------
@retry(max_attempts=3, backoff=1.0)
def list_sheets_titles(sheets_client, spreadsheet_id: str) -> List[str]:
    meta = sheets_client.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    return [s['properties']['title'] for s in meta.get('sheets', [])]

@retry(max_attempts=3, backoff=1.0)
def read_values(sheets_client, spreadsheet_id: str, a1_range: str) -> List[List[Any]]:
    res = sheets_client.values().get(spreadsheetId=spreadsheet_id, range=a1_range, majorDimension='ROWS').execute()
    return res.get('values', [])

@retry(max_attempts=3, backoff=1.0)
def append_values(sheets_client, spreadsheet_id: str, a1_range: str, values: List[List[Any]]):
    body = {"values": values}
    return sheets_client.values().append(spreadsheetId=spreadsheet_id, range=a1_range,
                                         valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body).execute()

@retry(max_attempts=3, backoff=1.0)
def update_values(sheets_client, spreadsheet_id: str, a1_range: str, values: List[List[Any]]):
    body = {"values": values}
    return sheets_client.values().update(spreadsheetId=spreadsheet_id, range=a1_range,
                                         valueInputOption='RAW', body=body).execute()

# ---------- friendly course name (optimized map) ----------
COURSE_MAP = [
    ("master 25-26", "Data Analytics"),
    ("master 25 26", "Data Analytics"),
    ("master comm", "Community Live Classes"),
    ("master community", "Community Live Classes"),
    ("master ds", "Data Science"),
    ("master data science", "Data Science"),
    ("master da", "Data Analytics"),
    ("da", "Data Analytics"),
    ("data analytics", "Data Analytics"),
    ("ds", "Data Science"),
    ("gen ai", "Generative AI"),
    ("genai", "Generative AI"),
    ("master gen ai", "Generative AI"),
    ("master fsd", "Full Stack Development"),
    ("fsd", "Full Stack Development"),
]

def friendly_course_name(sheet_name: str) -> str:
    if not sheet_name:
        return ""
    s = sheet_name.lower()
    for k,v in COURSE_MAP:
        if k in s:
            return v
    cleaned = re.sub(r'master', '', sheet_name, flags=re.I).replace('_',' ').replace('-',' ').strip()
    return " ".join([p.capitalize() for p in re.split(r'\s+', cleaned) if p])

# ---------- Aggregation (optimized: list sheets + bulk reads) ----------
def find_header_index(headers: List[str], keys: List[str]) -> int:
    if not headers:
        return -1
    low = [str(h).lower() for h in headers]
    for i,h in enumerate(low):
        for k in keys:
            if k in h:
                return i
    return -1

def aggregate_from_effort(sheets_client, effort_sheet_id: str, selected_month: str) -> Dict[str, Decimal]:
    totals: Dict[str, Decimal] = {}
    titles = list_sheets_titles(sheets_client, effort_sheet_id)
    for title in titles:
        if not re.search(r'master', title, re.I):
            continue
        # bulk read used range (A:Z to avoid overly large calls)
        a1 = f"'{title}'!A1:Z9999"
        vals = read_values(sheets_client, effort_sheet_id, a1)
        if not vals or len(vals) < 2:
            continue
        headers = vals[0]
        col_sme = find_header_index(headers, ["sme","instructor","name"])
        col_month = find_header_index(headers, ["month"])
        col_amt = find_header_index(headers, ["final round","final amount","amount"])
        if col_sme == -1 or col_amt == -1:
            continue
        course = friendly_course_name(title)
        for row in vals[1:]:
            month_cell = row[col_month] if col_month < len(row) else ""
            if month_cell and str(month_cell).strip() != str(selected_month).strip():
                continue
            sme = row[col_sme] if col_sme < len(row) else ""
            if not sme or str(sme).strip()=="":
                continue
            raw_amt = row[col_amt] if col_amt < len(row) else ""
            amt = parse_amount(raw_amt)
            key = f"{str(sme).strip()}|{course}"
            totals[key] = totals.get(key, Decimal("0")) + amt
    # normalize totals to Decimal and round
    return {k: round2_decimal(v) for k,v in totals.items()}

# ---------- SME links map (normalized) ----------
def build_sme_links_map(sheets_client, sme_sheet_id: str) -> Dict[str, Dict[str,Any]]:
    titles = list_sheets_titles(sheets_client, sme_sheet_id)
    links_map: Dict[str, Dict[str,Any]] = {}
    for title in titles:
        if title.lower() == 'onboarded':
            continue
        a1 = f"'{title}'!A1:Z9999"
        vals = read_values(sheets_client, sme_sheet_id, a1)
        if not vals or len(vals) < 2:
            continue
        for r in vals[1:]:
            raw_name = r[0] if len(r)>0 else ""
            if not raw_name or str(raw_name).strip()=="":
                continue
            name_norm = normalize_text(raw_name)
            if name_norm not in links_map:
                links_map[name_norm] = {"__any":[], "__email":"", "__courses":[]}
            email = r[1] if len(r)>1 else ""
            if email and not links_map[name_norm]["__email"]:
                links_map[name_norm]["__email"] = str(email).strip()
            url = ""
            # attempt to find url in row (common: col H or any cell that looks like https)
            for cell in r[2:10]:  # small scan
                if isinstance(cell, str) and re.match(r'^https?:\/\/', cell.strip(), re.I):
                    url = cell.strip(); break
            course = friendly_course_name(title)
            if url:
                links_map[name_norm][course] = url
                links_map[name_norm]["__any"].append(url)
            if course not in links_map[name_norm]["__courses"]:
                links_map[name_norm]["__courses"].append(course)
    return links_map

# ---------- Build master rows ----------
def build_master_rows(totals: Dict[str, Decimal], selected_month: str) -> List[List[Any]]:
    parsed = parse_month_string(selected_month)
    if parsed:
        period = f"1 {parsed['monthName']} - {parsed['lastDay']} {parsed['monthName']} {parsed['year']}"
        invoice_dated = parsed['lastDate'].strftime('%d/%m/%Y')
    else:
        period = selected_month
        invoice_dated = datetime.now().strftime('%d/%m/%Y')
    invoice_last_date = datetime.now().strftime('%d %m %Y')
    rows = []
    for key, amt in totals.items():
        sme, course = key.split('|',1)
        rows.append([
            selected_month,
            sme,
            course,
            "",                            # Tracker link (populated later)
            str(round2_decimal(Decimal(amt))), # amount as string
            period,
            invoice_last_date,
            "",                            # invoice number (to be set)
            invoice_dated,
            ""                             # email (populated later)
        ])
    return rows

# ---------- Populate tracker links (formula-based) ----------
def populate_tracker_links_and_emails(sheets_client, master_sheet_id: str, links_map: Dict[str,Any], master_sheet_name='Master data'):
    # read A:J range
    a1 = f"'{master_sheet_name}'!A1:J9999"
    vals = read_values(sheets_client, master_sheet_id, a1)
    if not vals or len(vals) < 2:
        return {"updated":0,"notFound":0,"emailsWritten":0}
    header = vals[0]; data = vals[1:]
    tracker_out = []
    email_out = []
    updated = notFound = emailsWritten = 0
    for row in data:
        instr = row[1] if len(row)>1 else ""
        course = row[2] if len(row)>2 else ""
        instr_norm = normalize_text(instr)
        found_url = ""
        found_email = ""
        if instr_norm in links_map:
            rec = links_map[instr_norm]
            # strict course-specific first, then any
            if course in rec:
                found_url = rec[course]
            elif rec.get("__any"):
                found_url = rec["__any"][0]
            found_email = rec.get("__email","")
        # tracker formula
        if found_url:
            tracker_out.append([f'=HYPERLINK("{found_url}","Tracker Link")'])
            updated += 1
        else:
            orig = row[3] if len(row)>3 else ""
            tracker_out.append([orig if orig else ""])
            if not orig:
                notFound += 1
        existing_email = row[9] if len(row)>9 else ""
        if found_email and str(found_email).strip() != str(existing_email).strip():
            email_out.append([found_email]); emailsWritten += 1
        else:
            email_out.append([existing_email if existing_email else ""])
    # bulk write
    update_values(sheets_client, master_sheet_id, f"'{master_sheet_name}'!D2:D{1+len(data)}", tracker_out)
    update_values(sheets_client, master_sheet_id, f"'{master_sheet_name}'!J2:J{1+len(data)}", email_out)
    return {"updated":updated,"notFound":notFound,"emailsWritten":emailsWritten}

# ---------- Invoice assignment (optimized, normalized matching with option to require exact) ----------
def assign_invoice_numbers(sheets_client, master_sheet_id: str, program_sheet_id: str,
                           master_sheet_name='Master data', program_sheet_name=None,
                           month_filter: Optional[str]=None, exact_match: bool=False, force_overwrite: bool=False):
    # Read master sheet A:K
    a1_master = f"'{master_sheet_name}'!A1:K9999"
    master_vals = read_values(sheets_client, master_sheet_id, a1_master)
    if not master_vals or len(master_vals) < 2:
        return {"status":"empty"}
    header = master_vals[0]; rows = master_vals[1:]
    # find indices robustly (case-insensitive)
    def index_of(hlist, candidates):
        low = [str(h).lower() for h in hlist]
        for c in candidates:
            for i,hh in enumerate(low):
                if c in hh:
                    return i
        return -1
    col_month = index_of(header, ["month"])
    col_sme = index_of(header, ["sme","instructor","name"])
    col_invoice = index_of(header, ["invoice number","invoice"])
    col_audit = index_of(header, ["invoice audit"])
    if col_sme == -1: col_sme = 1
    if col_invoice == -1: col_invoice = 7  # fallback to H
    # Read program sheet to build prevMax
    prog_range = f"'{program_sheet_name}'!A1:Z9999" if program_sheet_name else "A1:Z9999"
    prog_vals = read_values(sheets_client, program_sheet_id, prog_range)
    if not prog_vals or len(prog_vals) < 2:
        return {"status":"no_program_rows"}
    prog_header = prog_vals[0]; prog_rows = prog_vals[1:]
    prog_sme_idx = index_of(prog_header, ["sme name","sme name / company name","company name"])
    prog_inv_idx = index_of(prog_header, ["invoice number","invoice"])
    if prog_sme_idx == -1: prog_sme_idx = 2
    if prog_inv_idx == -1: prog_inv_idx = 1
    prev_max: Dict[str,int] = {}
    prog_refs: Dict[str,List[int]] = {}
    for i,row in enumerate(prog_rows):
        name = row[prog_sme_idx] if prog_sme_idx < len(row) else ""
        if not name: continue
        key = name if exact_match else normalize_text(name)
        inv_raw = row[prog_inv_idx] if prog_inv_idx < len(row) else ""
        m = re.search(r'(\d+)', str(inv_raw))
        num = int(m.group(1)) if m else 0
        if key not in prev_max or num > prev_max[key]:
            prev_max[key] = num
        prog_refs.setdefault(key, []).append(i+2)
    # Group master rows that match program SMEs
    groups: Dict[str,List[int]] = {}
    for i,row in enumerate(rows):
        sheet_row_number = i+2
        if month_filter and col_month != -1:
            month_val = row[col_month] if col_month < len(row) else ""
            if str(month_val).strip() != str(month_filter).strip():
                continue
        name = row[col_sme] if col_sme < len(row) else ""
        if not name: continue
        key = name if exact_match else normalize_text(name)
        if key in prev_max:
            groups.setdefault(key, []).append(sheet_row_number)
    if not groups:
        return {"status":"no_matches"}
    # prepare outputs for invoice and audit columns
    data_count = len(rows)
    invoice_out = [[rows[i][col_invoice] if col_invoice < len(rows[i]) else ""] for i in range(data_count)]
    audit_out = [[rows[i][col_audit] if col_audit < len(rows[i]) else ""] for i in range(data_count)]
    total_assigned = 0
    for key, row_nums in groups.items():
        next_num = (prev_max.get(key,0) or 0) + 1
        refs = prog_refs.get(key, [])
        for sheet_row in row_nums:
            idx = sheet_row - 2
            existing = invoice_out[idx][0]
            if existing and not force_overwrite:
                continue
            invoice_out[idx][0] = f"Invoice # {next_num}"
            if col_audit >= 0:
                audit_out[idx][0] = f"MatchedProgramRows:{','.join(map(str,refs)) or 'none'}; PrevInv:{prev_max.get(key,0)}; Assigned:{next_num}"
            next_num += 1
            total_assigned += 1
    # write back
    # convert column index to letter(s)
    def col_to_a1(col_idx:int) -> str:
        # col_idx is 0-based; returns A/B/.../AA etc.
        s = ""
        n = col_idx + 1
        while n > 0:
            n, r = divmod(n-1, 26)
            s = chr(65 + r) + s
        return s
    inv_col_letter = col_to_a1(col_invoice)
    audit_col_letter = col_to_a1(col_audit) if col_audit>=0 else None
    update_values(sheets_client, master_sheet_id, f"'{master_sheet_name}'!{inv_col_letter}2:{inv_col_letter}{1+data_count}", invoice_out)
    if audit_col_letter:
        update_values(sheets_client, master_sheet_id, f"'{master_sheet_name}'!{audit_col_letter}2:{audit_col_letter}{1+data_count}", audit_out)
    return {"status":"ok","assigned":total_assigned}
