# src/run_create_invoices.py
import os, sys, json
from google.oauth2 import service_account
from googleapiclient.discovery import build

from invoice_utils import (
    parse_month_string, aggregate_from_effort, build_master_rows,
    append_values, build_sme_links_map, populate_tracker_links_and_emails,
    assign_invoice_numbers
)

# Config via env (set in GitHub secrets)
EFFORT_SHEET_ID = os.getenv("EFFORT_SHEET_ID")
SME_SHEET_ID = os.getenv("SME_SHEET_ID")
MASTER_SHEET_ID = os.getenv("MASTER_SHEET_ID")
PROGRAM_SHEET_ID = os.getenv("PROGRAM_SHEET_ID")  # optional but recommended
GCP_SA_KEY = os.getenv("GCP_SA_KEY")
TZ = os.getenv("TZ", "UTC")
# Behavior flags
EXACT_MATCH_INVOICE = False    # set True if you want exact-match behavior like Apps Script
FORCE_OVERWRITE = False        # set True to overwrite existing invoice numbers
PROCESSED_FLAG_COLUMN = "Processed"  # optional: not auto-created; you can add later

if not GCP_SA_KEY:
    print("GCP_SA_KEY not set. Exiting.")
    sys.exit(1)

def get_sheets_client():
    sa_info = json.loads(GCP_SA_KEY)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return build('sheets', 'v4', credentials=creds).spreadsheets()

def run(selected_month: str):
    print("Starting invoice automation for month:", selected_month)
    sheets_client = get_sheets_client()
    # 1) aggregate
    print("Aggregating effort tracker...")
    totals = aggregate_from_effort(sheets_client, EFFORT_SHEET_ID, selected_month)
    if not totals:
        print("No totals found for month:", selected_month)
        return {"status":"empty"}
    # 2) build master rows
    rows = build_master_rows(totals, selected_month)
    print(f"Appending {len(rows)} rows to Master data.")
    append_values(sheets_client, MASTER_SHEET_ID, "'Master data'!A1", rows)
    # 3) build sme map & populate tracker links & emails
    print("Building SME links map...")
    links_map = build_sme_links_map(sheets_client, SME_SHEET_ID)
    try:
        stats = populate_tracker_links_and_emails(sheets_client, MASTER_SHEET_ID, links_map)
        print("Tracker populate stats:", stats)
    except Exception as e:
        print("populate tracker failed:", str(e))
    # 4) assign invoice numbers
    if PROGRAM_SHEET_ID:
        print("Assigning invoice numbers (exact_match=%s)..." % (EXACT_MATCH_INVOICE,))
        res = assign_invoice_numbers(sheets_client, MASTER_SHEET_ID, PROGRAM_SHEET_ID,
                                     master_sheet_name='Master data',
                                     program_sheet_name=None,
                                     month_filter=selected_month,
                                     exact_match=EXACT_MATCH_INVOICE,
                                     force_overwrite=FORCE_OVERWRITE)
        print("Invoice assignment result:", res)
    else:
        print("PROGRAM_SHEET_ID not provided; skipping invoice assignment.")
    return {"status":"ok","written":len(rows)}

if __name__ == "__main__":
    month = sys.argv[1] if len(sys.argv)>1 else input("Enter month (e.g. September 2025): ")
    out = run(month)
    print("Finished:", out)
