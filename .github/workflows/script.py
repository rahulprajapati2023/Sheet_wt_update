import mysql.connector
import gspread
import json
import os
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURATION
# ============================================================
DB_CONFIG = {
    "host":     "103.195.186.17",
    "port":     3306,
    "database": "wt_marketing",
    "user":     "rahul",
    "password": "t3#Zw390r",
}

SPREADSHEET_NAME = "Invoice"
SHEET_TAB_NAME   = "Invoice"

# Load credentials from credentials.json sitting next to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(SCRIPT_DIR, "credentials.json")

with open(CREDS_FILE, "r") as f:
    GSHEET_CREDENTIALS = json.load(f)

HEADERS = [
    "Date", "Customer Name", "Source",
    "Store Name", "Dealer Code", "Sub Category",
    "Main Category", "Qty", "Sale", "Primary Phone"
]


def get_yesterday():
    return (datetime.today() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

def get_month_start(date):
    return date.replace(day=1)

def get_month_end(date):
    if date.month == 12:
        return date.replace(day=31)
    return date.replace(day=1, month=date.month + 1) - timedelta(days=1)

def format_date(date):
    return date.strftime("%Y-%m-%d")


def fetch_data():
    yesterday = get_yesterday()
    start_str = format_date(get_month_start(yesterday))
    end_str   = format_date(get_month_end(yesterday))
    print(f"Fetching data: {start_str} to {end_str}")

    query = f"""
        SELECT 
            date, `Customer Name`, SOURCE,
            CASE LOWER(TRIM(SOURCE))
                WHEN 'dealer ap'                               THEN 'Others'
                WHEN 'wallace garden chennai wt'               THEN 'Chennai Store'
                WHEN 'lido store wt'                           THEN 'Lido Store'
                WHEN 'bellary road wt'                         THEN 'Hebbal Store'
                WHEN 'ahmedabad store'                         THEN 'Ahmedabad Store'
                WHEN 'online wt'                               THEN 'Online'
                WHEN 'kapila lighting studio (foco)'           THEN 'Ludhiana Store'
                WHEN 'customer care'                           THEN 'Online'
                WHEN 'vendor-ap'                               THEN 'Others'
                WHEN 'illume (atpl foco)'                      THEN 'Preet Vihar Store'
                WHEN 'jaiswal  foco'                           THEN 'Kolkata Store'
                WHEN 'andheri wt'                              THEN 'Andheri Store'
                WHEN 'mg road wt'                              THEN 'MG Store'
                WHEN 'ap - bungalow'                           THEN 'Others'
                WHEN 'karnataka hardware and paints (foco)'    THEN 'Jayanagar Store'
                WHEN 'cochin store'                            THEN 'Cochin store'
                WHEN 'ring road (lajpat nagar iv) store'       THEN 'Lajpat Nagar Store'
                WHEN 'boatclub pune wt'                        THEN 'Pune Store'
                WHEN 'kirti nagar store'                       THEN 'Kirti Nagar Store'
                WHEN 'lower parel wt'                          THEN 'Lower Parel Store'
                WHEN 'coimbatore store'                        THEN 'Coimbatore Store'
                WHEN 'banjara hills wt'                        THEN 'Banjara Hills Store'
                WHEN 'vadodara store'                          THEN 'Vadodara store'
                WHEN 'srinivasa lights_gachibowlistore (foco)' THEN 'Gachibowli Store'
                WHEN 'stanley stores'                          THEN 'Stanley'
                WHEN 'bh service'                              THEN 'Others'
                WHEN 'project sales - ap'                      THEN 'Others'
                WHEN 'ap plant'                                THEN 'Others'
                WHEN 'ap ho'                                   THEN 'Others'
                WHEN 'inside sales team'                       THEN 'Others'
                WHEN 'thane store wt'                          THEN 'Thane Store'
                WHEN 'bhs dealer'                              THEN 'Others'
                WHEN 'outbound team'                           THEN 'Online'
                WHEN 'wt-dubai'                                THEN 'Others'
                WHEN 'internal invoice'                        THEN 'Others'
                WHEN 'twt studio (foco)'                       THEN 'Kirti Nagar Store'
                WHEN 'vashi store'                             THEN 'Vashi Store'
                WHEN 'navneet enterprises (foco)'              THEN 'Raipur Store'
                WHEN 'bh service al'                           THEN 'Others'
                WHEN 'ap-color idea'                           THEN 'Others'
                WHEN 'indore store'                            THEN 'Indore store'
                WHEN 'bhs events'                              THEN 'Others'
                WHEN 'calicut store'                           THEN 'Others'
                WHEN 'wave city noida wt'                      THEN 'Noida Store'
                WHEN 'bgs foco'                                THEN 'Amritsar Store'
                WHEN 'udc foco'                                THEN 'Dealer AP'
                WHEN 'boat clube pune wt'                      THEN 'Pune Store'
                WHEN 'ap store'                                THEN 'Others'
                WHEN 'kochi store'                             THEN 'Cochin store'
                WHEN 'ap bungalow'                             THEN 'Others'
                WHEN 'architectural lightings'                 THEN 'Architectural lightings'
                ELSE 'Not Mapped'
            END AS Store_Name,
            `DEALER CODE`, sub_category,
            CASE LOWER(sub_category)
                WHEN 'floor lamp'               THEN 'Decore Lights'
                WHEN 'table lamp'               THEN 'Decore Lights'
                WHEN 'HANGING LIGHT/CHANDELIER' THEN 'Decore Lights'
                WHEN 'chandelier'               THEN 'Decore Lights'
                WHEN 'wall lights'              THEN 'Decore Lights'
                WHEN 'ceiling light'            THEN 'Decore Lights'
                WHEN 'pendant light'            THEN 'Decore Lights'
                WHEN 'down light'               THEN 'Functional Light'
                WHEN 'strip light'              THEN 'Functional Light'
                WHEN 'led driver'               THEN 'Functional Light'
                WHEN 'gate light'               THEN 'Functional Light'
                WHEN 'functional light'         THEN 'Functional Light'
                WHEN 'garden light'             THEN 'Functional Light'
                WHEN 'staircase light'          THEN 'Functional Light'
                WHEN 'flood light'              THEN 'Functional Light'
                WHEN 'spare parts'              THEN 'Accessories'
                WHEN 'bulbs'                    THEN 'Accessories'
                WHEN 'installation'             THEN 'Accessories'
                WHEN 'ceiling fan'              THEN 'Fans'
                WHEN 'wall fan'                 THEN 'Fans'
                WHEN 'table fan'                THEN 'Fans'
                WHEN 'decor product'            THEN 'Decore Artefacts'
                WHEN 'planters'                 THEN 'Decore Artefacts'
                WHEN 'vases'                    THEN 'Decore Artefacts'
                WHEN 'sculptures'               THEN 'Decore Artefacts'
                WHEN 'trays'                    THEN 'Decore Artefacts'
                WHEN 'wall paintings'           THEN 'Decore Artefacts'
                WHEN 'candle holders'           THEN 'Decore Artefacts'
                WHEN 'tissue'                   THEN 'Decore Artefacts'
                WHEN 'decorative boxes'         THEN 'Decore Artefacts'
                WHEN 'wall art'                 THEN 'Decore Artefacts'
                WHEN 'mirror'                   THEN 'Decore Artefacts'
                WHEN 'clocks'                   THEN 'Decore Artefacts'
                WHEN 'urli'                     THEN 'Decore Artefacts'
                WHEN 'sofa'                     THEN 'Furnitures'
                WHEN 'chairs'                   THEN 'Furnitures'
                WHEN 'table'                    THEN 'Furnitures'
                WHEN 'rug'                      THEN 'Furnitures'
                WHEN 'side table'               THEN 'Furnitures'
                ELSE 'Not Mapped'
            END AS Main_Category,
            SUM(`Quantity Ordered`) AS Qty,
            SUM(`Total (BCY)`)      AS Sale,
            `Primary Phone`
        FROM Invoice
        WHERE
            `Customer Name` NOT LIKE '%White teak%'
            AND `Customer Name` NOT LIKE '%Whiteteak%'
            AND `Source`        NOT LIKE '%Debit Note%'
            AND date >= '{start_str}'
            AND date <= '{end_str}'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 10
    """

    conn   = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    data = [[str(c) if c is not None else "" for c in row] for row in rows]
    print(f"Fetched {len(data)} rows from DB.")
    return data


def find_current_month_block(data_rows, month_start, month_end):
    first_idx = last_idx = -1
    for i, row in enumerate(data_rows):
        raw = row[0] if row else ""
        if not raw:
            continue
        try:
            cell_date = datetime.strptime(str(raw).strip()[:10], "%Y-%m-%d")
        except ValueError:
            continue
        if month_start <= cell_date <= month_end:
            if first_idx == -1:
                first_idx = i
            last_idx = i
    return first_idx, last_idx


def update_sheet(new_data):
    yesterday   = get_yesterday()
    month_start = get_month_start(yesterday)
    month_end   = get_month_end(yesterday)

    print("Connecting to Google Sheets...")
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds     = Credentials.from_service_account_info(GSHEET_CREDENTIALS, scopes=scopes)
    gc        = gspread.authorize(creds)
    sh        = gc.open(SPREADSHEET_NAME)
    worksheet = sh.worksheet(SHEET_TAB_NAME)

    print("Reading existing sheet data...")
    all_values = worksheet.get_all_values()

    if not all_values:
        worksheet.update("A1", [HEADERS] + new_data, value_input_option="USER_ENTERED")
        print(f"Written fresh: {len(new_data)} rows.")
        return

    header_row = all_values[0]
    data_rows  = all_values[1:]
    first_idx, last_idx = find_current_month_block(data_rows, month_start, month_end)

    if first_idx == -1:
        prior_rows = data_rows
    else:
        prior_rows = data_rows[:first_idx] + data_rows[last_idx + 1:]
        print(f"Removed {last_idx - first_idx + 1} current month rows.")

    final_data = [header_row] + prior_rows + new_data
    print(f"Writing {len(final_data) - 1} total rows to sheet...")
    worksheet.clear()
    worksheet.update("A1", final_data, value_input_option="USER_ENTERED")
    print(f"✅ Done! Prior: {len(prior_rows)} rows | New: {len(new_data)} rows")


def main():
    try:
        print("Starting refresh...")
        new_data = fetch_data()
        update_sheet(new_data)
        print("✅ All done!")
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
