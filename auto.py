import gspread
from google.oauth2.service_account import Credentials
import schedule
import mysql.connector as mysql
from datetime import date, datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


HOST = "your host"
DATABASE ="your database name"
USER ="your database user"
PASSWORD = "your data base password"
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

Sheet_ID = "your sheet ID"

def connect_to_db():
  try:
    con = mysql.connect(
        host=HOST, database=DATABASE, user=USER, password=PASSWORD
    )
    return con
  except mysql.Error as err:
    print(f"Error connecting to database: {err}")
    return None

def connect_to_sheet():
    try:
        creds = Credentials.from_service_account_file(
            "your json file name.json",
            scopes=SCOPES
        )
        client = gspread.authorize(creds)
        workbook = client.open_by_key(Sheet_ID)
        print(f"Worksheets available: {[ws.title for ws in workbook.worksheets()]}")
        return workbook.worksheet("your sheet name")
    except Exception as err:
        print(f"Error connecting to Google Sheet: {err}")
        return None


def transfer_data():
    db_con = connect_to_db()
    sheet = connect_to_sheet()

    if db_con and sheet:
        try:
            cursor = db_con.cursor()

            query = """
                SELECT ID, name, first_call_agent,first_call_status,second_call_agent,second_call_status, phone_number, block_num, 
                        aware_of_top_up, 
                       top_up_method, reason_for_not_topping_up
                FROM captins
                ORDER BY created_at DESC
                LIMIT 25;
            """
            cursor.execute(query)
            rows = cursor.fetchall()

            headers = [
                "ID", "Name", "First Call Agent","First Call Status","Second Call Agent","Second Call Status", "Phone Number", "Block Number",  
                 "Registration Date", 
                "Aware of Top-Up", "Top-Up Method", "Reason for Not Topping Up"
            ]
            def convert_row(row):
                current_date = datetime.now().strftime("%Y-%m-%d")
                return [current_date] + [str(item) if isinstance(item, (date,)) else item for item in row]
        
            data =  [convert_row(row) for row in rows]
            next_row = len(sheet.get_all_values()) + 1

            sheet.update(f"A{next_row}", data)

            logging.info("Data transferred to Google Sheet successfully!")
        except mysql.Error as err:
            logging.error(f"error:{err}")

            print(f"Error fetching data from MySQL: {err}")
        except Exception as err:
            logging.error(f"error:{err}")

            print(f"Error updating Google Sheet: {err}")
        finally:
            db_con.close()
    else:
        print("Failed to connect to database or Google Sheet.")


schedule.every(15).minutes.do(transfer_data)

while True:
  schedule.run_pending()




