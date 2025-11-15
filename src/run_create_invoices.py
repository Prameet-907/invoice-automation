# src/run_create_invoices.py
# Python implementation of your Apps Script pipeline.
# Requirements: google-api-python-client, google-auth, gspread (optional)
# Reads GCP_SA_KEY from env, plus MASTER_SHEET_ID, EFFORT_SHEET_ID, SME_SHEET_ID, PROGRAM_SHEET_ID.

import os
import re
import json
import sys
from datetime import datetime
from math import isfinite
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from typing import List, Dict, Tuple

# --- Config / column mapping (1-based columns) ---
MASTER_SHEET_NAME = "Master data"
CONFIG_RANGE_MONTH = "'Config'!A1"
CONFIG_RANGE_PROCESSED = "'Config'!B1"

# Master data columns (1-based)
COL_MONTH = 1     # A
COL_SME = 2       # B
COL_COURSE = 3    # C
COL_TRACKER = 4   # D  (we will write HYPERLINK formula)
COL_AMOUNT = 5    # E
COL_PERIOD = 6    # F
COL_INV_LAST_DATE = 7  # G
COL_INV_NUMBER = 8     # H
COL_INV_DATED = 9      # I
COL_EMAIL = 10         # J

# --- Scopes & env ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

MASTER_SHEET_ID = os.environ.get("MASTER_SHEET_ID")
EFFORT_SHEET_ID = os.environ.get("EFFORT_SHEET_ID")
SME_SHEET_ID = os.environ.get("SME_SHEET_ID")
PROGRAM_SHEET_ID = os.environ.get("PROGRAM_SHEET_ID")
GCP_SA_KEY = os.environ.get("GCP_SA_KEY")

if not GCP_SA_KEY:
    raise SystemExit("Missing GCP_SA_KEY env")

# --- Helpers: Authorization / Sheets client ---
def get_sheets_service():
    info = json.loads(GCP_SA_KEY)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    return service

# Utility: find col index by keywords in header (0-based)
def find_col(headers: List[str], keywords: List[str]) -> int:
    hlower = [str(h or "").lower() for h in headers]
    for i, h in enumerate(hlower):
        for k in keywords:
            if k in h:
                return i
    return -1

def round2(n):
    try:
        return round(float(n) + 1e-9, 2)
    except Exception:
        return 0.0

# --- 1) Read month from Config!A1 ---
def read_month_from_config(svc) -> str:
    res = svc.spreadsheets().values().get(spreadsheetId=MASTER_SHEET_ID, range=CONFIG_RANGE_MONTH).execute()
    vals = res.get("values", [])
    if vals and vals[0] and str(vals[0][0]).strip():
        return str(vals[0][0]).strip()
    return None

def mark_processed(svc, month: str):
    ts = datetime.utcnow().isoformat() + "Z"
    svc.spreadsheets().values().update(
        spreadsheetId=MASTER_SHEET_ID, range=CONFIG_RANGE_PROCESSED,
        valueInputOption="RAW", body={"values": [[f"Processed {month} at {ts}"]]}
    ).execute()

# --- 2) Aggregate from Effort Tracker: scan sheets whose name contains 'master' ---
def aggregate_from_effort(svc, selected_month: str) -> Dict[Tuple[str,str], float]:
    """
    Returns totals dict keyed by (sme, course) -> total amount (float)
    """
    totals = {}
    # read spreadsheet metadata to iterate sheets
    meta = svc.spreadsheets().get(spreadsheetId=EFFORT_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    for sh in sheets:
        title = sh.get("properties", {}).get("title", "")
        if not re.search(r"master", title, re.I):
            continue
        # read the sheet range fully (safe approach: get used range)
        # Get gridProperties to know rowCount/colCount? We'll request a large block (1..lastRow,lastCol)
        sheet_title = title.replace("'", "\\'")
        # read first 500 rows (like Apps Script)
        rng = f"'{title}'!A1:Z500"
        try:
            resp = svc.spreadsheets().values().get(spreadsheetId=EFFORT_SHEET_ID, range=rng).execute()
            data = resp.get("values", [])
            if not data or len(data) < 2:
                continue
            headers = [str(h or "") for h in data[0]]
            col_sme = find_col(headers, ["sme", "instructor", "name"])
            col_month = find_col(headers, ["month"])
            col_final = find_col(headers, ["final round", "final amount", "amount"])
            if col_sme == -1 or col_final == -1:
                continue
            for row in data[1:]:
                month_cell = row[col_month] if col_month < len(row) else ""
                if str(month_cell).strip() == "" or str(month_cell).strip() != selected_month:
                    continue
                sme = str(row[col_sme]).strip() if col_sme < len(row) else ""
                if not sme:
                    continue
                raw_amt = row[col_final] if col_final < len(row) else ""
                amt = 0.0
                try:
                    amt_text = re.sub(r"[^0-9\.\-]", "", str(raw_amt) or "0")
                    amt = float(amt_text) if amt_text not in ("", ".", "-") else 0.0
                except Exception:
                    amt = 0.0
                course = friendly_course_name(title)
                key = (sme, course)
                totals[key] = totals.get(key, 0.0) + amt
        except Exception as e:
            print("Warning reading sheet", title, ":", e)
            continue
    return totals

# friendly_course_name replicates Apps Script mapping heuristics
def friendly_course_name(sheet_name: str) -> str:
    if not sheet_name:
        return ""
    s = sheet_name.lower()
    map_pairs = [
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
        ("full stack", "Full Stack Development"),
        ("master full stack", "Full Stack Development"),
    ]
    for k,v in map_pairs:
        if k in s:
            return v
    # fallback: remove "master" and title-case
    cleaned = re.sub(r"master", "", sheet_name, flags=re.I)
    cleaned = re.sub(r"[_\-]+", " ", cleaned).strip()
    parts = [w.capitalize() for w in cleaned.split() if w.strip()]
    return " ".join(parts) or sheet_name

# --- 3) Build SME links map from SME management sheet ---
def build_sme_links_map(svc) -> Dict[str, Dict]:
    """
    Returns linksMap keyed by normalized instructor name -> record dict:
      { '__any': [urls], '__email': email, '__courses': [courseNames], '<CourseName>': url }
    Normalization: NFKC + lower + collapse spaces
    """
    links_map = {}
    # Read entire sheet (first ~5000 rows safe)
    rng = "A1:Z5000"
    resp = svc.spreadsheets().values().get(spreadsheetId=SME_SHEET_ID, range=rng).execute()
    data = resp.get("values", [])
    if not data or len(data) < 2:
        return {"linksMap": links_map, "rowsScanned": 0}
    # We assume first row is header; iterate through sheets in SME_SS where each sheet is a course
    # But since SME_SHEET_ID is a single spreadsheet with multiple tabs, we need to fetch sheet names to iterate tabs
    meta = svc.spreadsheets().get(spreadsheetId=SME_SHEET_ID).execute()
    sheets = meta.get("sheets", [])
    rows_scanned = 0
    for sh in sheets:
        title = sh.get("properties", {}).get("title", "")
        # skip "Onboarded" etc if present per Apps Script
        if title.lower() == "onboarded":
            continue
        friendly = friendly_course_name(title)
        # read this sheet
        rng_s = f"'{title}'!A2:H5000"
        try:
            resp_s = svc.spreadsheets().values().get(spreadsheetId=SME_SHEET_ID, range=rng_s).execute()
            rows = resp_s.get("values", [])
            for r in rows:
                if not r or len(r) == 0:
                    continue
                raw_name = r[0]
                if not raw_name or str(raw_name).strip() == "":
                    continue
                rows_scanned += 1
                name_norm = normalize_name(raw_name)
                if name_norm not in links_map:
                    links_map[name_norm] = {"__any": [], "__email": "", "__courses": []}
                candidate_email = (r[1] if len(r) > 1 else "") or ""
                if candidate_email and not links_map[name_norm]["__email"]:
                    links_map[name_norm]["__email"] = str(candidate_email).strip()
                # column H (index 7) is the rich/url cell in Apps Script; here we check index 7 (0-based)
                url = ""
                if len(r) >= 8:
                    cand = r[7]
                    if isinstance(cand, str) and cand.strip().lower().startswith("http"):
                        url = cand.strip()
                # fallback: sometimes column 8 may be a plain URL in other columns - ignore for now
                if url:
                    links_map[name_norm][friendly] = url
                    links_map[name_norm]["__any"].append(url)
                if friendly not in links_map[name_norm]["__courses"]:
                    links_map[name_norm]["__courses"].append(friendly)
        except Exception as e:
            print("Warning fetching SME sheet", title, e)
            continue
    return {"linksMap": links_map, "rowsScanned": rows_scanned}

def normalize_name(x):
    return re.sub(r"\s+", " ", str(x or "").strip()).lower()

# --- 4) Append master rows to Master data ---
def append_master_rows(svc, rows: List[List]):
    if not rows:
        return {"status":"empty","written":0}
    # fetch current header to detect insertion row
    meta = svc.spreadsheets().get(spreadsheetId=MASTER_SHEET_ID).execute()
    # append at bottom
    range_append = f"'{MASTER_SHEET_NAME}'!A1"
    # Use valueInputOption RAW so numbers remain numbers where possible
    body = {"values": rows}
    resp = svc.spreadsheets().values().append(
        spreadsheetId=MASTER_SHEET_ID, range=range_append,
        valueInputOption="RAW", insertDataOption="INSERT_ROWS", body=body
    ).execute()
    updates = resp.get("updates", {})
    written = updates.get("updatedRows", 0)
    return {"status":"ok","written": written}

# --- 5) Populate tracker links and emails (writes =HYPERLINK(url,"Tracker Link") into D; email into J) ---
def populate_tracker_links_and_emails(svc):
    # Read master sheet range A:J first ~5000 rows
    rng = f"'{MASTER_SHEET_NAME}'!A1:J5000"
    resp = svc.spreadsheets().values().get(spreadsheetId=MASTER_SHEET_ID, range=rng, majorDimension="ROWS").execute()
    data = resp.get("values", [])
    if not data or len(data) < 2:
        return {"updated":0,"notFound":0,"emailsWritten":0,"unmatchedSample":[]}
    headers = data[0]
    rows = data[1:]
    num = len(rows)
    # Build links map once
    sme_info = build_sme_links_map(svc)
    links_map = sme_info.get("linksMap", {})
    tracker_out = []
    email_out = []
    updated = 0
    notFound = 0
    emailsWritten = 0
    unmatchedSample = []
    for r in rows:
        instr = r[COL_SME-1] if len(r) >= COL_SME else ""
        course = r[COL_COURSE-1] if len(r) >= COL_COURSE else ""
        instr_norm = normalize_name(instr)
        found_url = ""
        found_email = ""
        if instr_norm and instr_norm in links_map:
            record = links_map[instr_norm]
            if course and course in record:
                found_url = record.get(course, "")
            if not found_url and record.get("__any"):
                # Apps Script avoided fallback; keep same behavior and do NOT fallback
                found_url = ""
            found_email = record.get("__email", "")
        # Build values
        if found_url:
            # use hyperlink formula so Sheets shows clickable text
            tracker_formula = f'=HYPERLINK("{found_url}","Tracker Link")'
            tracker_out.append([tracker_formula])
            updated += 1
        else:
            # keep existing D value if present
            existing = r[COL_TRACKER-1] if len(r) >= COL_TRACKER else ""
            tracker_out.append([existing if existing else ""])
            if not existing:
                notFound += 1
        # Email
        existing_email = r[COL_EMAIL-1] if len(r) >= COL_EMAIL else ""
        if found_email and str(found_email).strip() != str(existing_email).strip():
            email_out.append([found_email])
            emailsWritten += 1
        else:
            email_out.append([existing_email if existing_email else ""])
    # perform batch updates: columns D and J
    svc.spreadsheets().values().update(
        spreadsheetId=MASTER_SHEET_ID,
        range=f"'{MASTER_SHEET_NAME}'!D2:D{1+num}",
        valueInputOption="USER_ENTERED",
        body={"values": tracker_out}
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=MASTER_SHEET_ID,
        range=f"'{MASTER_SHEET_NAME}'!J2:J{1+num}",
        valueInputOption="RAW",
        body={"values": email_out}
    ).execute()
    # build unmatched sample
    for idx, outcell in enumerate(tracker_out[:30]):
        t = outcell[0] if outcell and len(outcell)>0 else ""
        if not t or str(t).strip() == "":
            rownum = 2 + idx
            unmatchedSample.append({"row": rownum, "instructor": rows[idx][COL_SME-1] if len(rows[idx])>=COL_SME else "", "course": rows[idx][COL_COURSE-1] if len(rows[idx])>=COL_COURSE else ""})
    return {"updated": updated, "notFound": notFound, "emailsWritten": emailsWritten, "unmatchedSample": unmatchedSample}

# --- 6) Assign invoice numbers (Exact match behavior) ---
def assign_invoice_numbers_exact_match(svc, force_overwrite=False, month_filter=None):
    # Read master sheet fully (A..J)
    rng_master = f"'{MASTER_SHEET_NAME}'!A1:K5000"
    resp = svc.spreadsheets().values().get(spreadsheetId=MASTER_SHEET_ID, range=rng_master).execute()
    vals = resp.get("values", [])
    if not vals or len(vals) < 2:
        print("No data in master")
        return {"assigned":0}
    headers = vals[0]
    rows = vals[1:]
    # Find indices in master by header names if possible
    try:
        col_month_index = headers.index("Month") if "Month" in headers else COL_MONTH-1
    except ValueError:
        col_month_index = COL_MONTH-1
    try:
        col_sme_index = headers.index("SME Name") if "SME Name" in headers else COL_SME-1
    except ValueError:
        col_sme_index = COL_SME-1
    try:
        col_invoice_index = headers.index("invoice number") if "invoice number" in headers else COL_INV_NUMBER-1
    except ValueError:
        col_invoice_index = COL_INV_NUMBER-1
    # PROGRAM sheet prev max map
    prog_rng = "A1:Z5000"
    prog_resp = svc.spreadsheets().values().get(spreadsheetId=PROGRAM_SHEET_ID, range=prog_rng).execute()
    prog_vals = prog_resp.get("values", [])
    if not prog_vals or len(prog_vals) < 2:
        print("No data in program sheet")
        return {"assigned":0}
    prog_header = prog_vals[0]
    # find prog columns
    prog_inv_idx = -1
    prog_sme_idx = -1
    for i,h in enumerate(prog_header):
        if isinstance(h, str) and "Invoice Number".lower() in h.lower():
            prog_inv_idx = i
        if isinstance(h, str) and "SME Name" in h or "SME Name / Company Name".lower() in str(h).lower():
            prog_sme_idx = i
    if prog_inv_idx == -1:
        prog_inv_idx = 1  # fallback to column B
    if prog_sme_idx == -1:
        prog_sme_idx = 2  # fallback to column C
    # build prevMaxInvoice map (exact string keys)
    prevMaxInvoice = {}
    matchedProgramRows = {}
    for i, r in enumerate(prog_vals[1:]):
        rawName = str(r[prog_sme_idx]) if prog_sme_idx < len(r) else ""
        if rawName is None or str(rawName).strip() == "":
            continue
        key = str(rawName)
        rawInv = r[prog_inv_idx] if prog_inv_idx < len(r) else ""
        num = 0
        if rawInv not in ("", None):
            m = re.search(r"(\d+)", str(rawInv))
            if m:
                try:
                    num = int(m.group(1))
                except:
                    num = 0
        prevMaxInvoice[key] = max(prevMaxInvoice.get(key, 0), num)
        matchedProgramRows.setdefault(key, []).append(i+2)  # sheet rows (1-based)
    # group master rows by exact SME name but only for SMEs present in prevMaxInvoice
    groups = {}
    for idx, row in enumerate(rows):
        sheet_row_num = idx + 2
        if month_filter and col_month_index < len(row):
            if str(row[col_month_index]).strip() != str(month_filter).strip():
                continue
        rawSme = row[col_sme_index] if col_sme_index < len(row) else ""
        if rawSme in ("", None):
            continue
        key = str(rawSme)
        if key in prevMaxInvoice:
            groups.setdefault(key, []).append(sheet_row_num)
    if not groups:
        print("No master rows to update (no matching SMEs in program sheet)")
        return {"assigned":0}
    # Build invoiceOut and auditOut arrays size = dataRowCount
    dataRowCount = len(rows)
    invoiceOut = [[rows[i][col_invoice_index] if col_invoice_index < len(rows[i]) else ""] for i in range(dataRowCount)]
    audit_out = [["" for _ in range(1)] for _ in range(dataRowCount)]
    total_assigned = 0
    for key, sheet_rows_for_key in groups.items():
        nextNum = (prevMaxInvoice.get(key, 0) or 0) + 1
        prog_refs = matchedProgramRows.get(key, [])
        for sheetRow in sheet_rows_for_key:
            zero_idx = sheetRow - 2
            existingInv = invoiceOut[zero_idx][0]
            if existingInv and not force_overwrite:
                continue
            invString = f"Invoice # {nextNum}"
            invoiceOut[zero_idx][0] = invString
            auditVal = f"MatchedProgramRows:{','.join(map(str,prog_refs)) or 'none'}; PrevInv:{prevMaxInvoice.get(key,0)}; Assigned:{nextNum}"
            audit_out[zero_idx][0] = auditVal
            nextNum += 1
            total_assigned += 1
    # write back invoice column and audit if any
    # invoice column is H -> 8 -> range start row 2 column 8
    svc.spreadsheets().values().update(
        spreadsheetId=MASTER_SHEET_ID,
        range=f"'{MASTER_SHEET_NAME}'!H2:H{1+dataRowCount}",
        valueInputOption="RAW",
        body={"values": invoiceOut}
    ).execute()
    # Audit column: if header had 'Invoice Audit (ProgramRow_INV3)' then we should find it; for now write to column K if present
    # Attempt to find audit header index
    audit_idx = -1
    for i,h in enumerate(headers):
        if isinstance(h,str) and "Invoice Audit".lower() in h.lower():
            audit_idx = i
            break
    if audit_idx >= 0:
        col_letter = chr(ord('A') + audit_idx)
        svc.spreadsheets().values().update(
            spreadsheetId=MASTER_SHEET_ID,
            range=f"'{MASTER_SHEET_NAME}'!{col_letter}2:{col_letter}{1+dataRowCount}",
            valueInputOption="RAW",
            body={"values": audit_out}
        ).execute()
    return {"assigned": total_assigned}

# --- Orchestration ---
def run(selected_month: str):
    svc = get_sheets_service()
    print("Aggregating from effort for:", selected_month)
    totals = aggregate_from_effort(svc, selected_month)
    rows = []
    tz = "UTC"
    parsed_period = None
    # build period & invoice dates approximations
    # If Apps Script parseMonthString was used, we produce a similar period string if month like 'September 2025'
    m = re.match(r"([A-Za-z]+)\s+(\d{4})", selected_month)
    if m:
        monthName = m.group(1)
        year = int(m.group(2))
        # last day calculation approximate (not timezone dependent)
        import calendar
        mindex = list(calendar.month_name).index(monthName) if monthName in list(calendar.month_name) else None
        if mindex:
            last_day = calendar.monthrange(year, mindex)[1]
            period = f"1 {monthName} - {last_day} {monthName} {year}"
            invoiceLastDate = datetime.utcnow().strftime("%d %m %Y")
            invoiceDated = f"{last_day:02d}/{mindex:02d}/{year}"
        else:
            period = selected_month
            invoiceLastDate = datetime.utcnow().strftime("%d %m %Y")
            invoiceDated = invoiceLastDate
    else:
        period = selected_month
        invoiceLastDate = datetime.utcnow().strftime("%d %m %Y")
        invoiceDated = invoiceLastDate
    for (sme, course), amount in totals.items():
        rows.append([
            selected_month,   # A Month
            sme,              # B SME
            course,           # C Course
            "",               # D tracker (to be filled)
            round2(amount),   # E Amount
            period,           # F Period
            invoiceLastDate,  # G Invoice Last Date
            "",               # H Invoice Number (assigned later)
            invoiceDated,     # I Invoice Dated
            ""                # J Email (to be updated)
        ])
    if not rows:
        print("No data found for that month.")
        return {"status":"empty"}
    print("Appending", len(rows), "rows to Master data")
    append_res = append_master_rows(svc, rows)
    print("Append result:", append_res)
    # populate tracker links & emails (best-effort)
    try:
        pop = populate_tracker_links_and_emails(svc)
        print("Populate tracker result:", pop.get("updated"), "updated,", pop.get("emailsWritten"), "emails")
    except Exception as e:
        print("Populate failed:", e)
    # assign invoice numbers (exact match behavior)
    try:
        assign_res = assign_invoice_numbers_exact_match(svc, force_overwrite=False, month_filter=selected_month)
        print("Assigned invoices:", assign_res.get("assigned"))
    except Exception as e:
        print("Invoice assignment failed:", e)
    # mark processed
    mark_processed(svc, selected_month)
    return {"status":"ok","written": append_res.get("written",0)}

# --- CLI entry ---
if __name__ == "__main__":
    # Accept optional CLI arg month, else read Config!A1
    svc = get_sheets_service()
    month = None
    if len(sys.argv) > 1:
        month = sys.argv[1]
    else:
        month = read_month_from_config(svc)
    if not month:
        print("No month selected (Config!A1 empty and no CLI arg). Exiting.")
        sys.exit(0)
    print("Selected month:", month)
    out = run(month)
    print("Done:", out)
