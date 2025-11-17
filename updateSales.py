import mysql.connector
import openpyxl
from datetime import datetime

"""This code demonstrates a workflow for reading Excel sales data and updating a MySQL database. This is all locally on the machine."""

#sqldb
connection = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)
cursor = connection.cursor()

#load sales from excel
file_path = "xlsx/sales2025.xlsx"
wb = openpyxl.load_workbook(file_path)
sheet = wb.active

#map payment method names to their database ids if exists
cursor.execute("SELECT id, method_name FROM payment_methods")
payment_method_map = {name: id for id, name in cursor.fetchall()}

sales_data = []

#etl method
for row in sheet.iter_rows(min_row=2, values_only=True):
    if len(row) < 4:
        continue

    order_id, payment_type, total, date_added = row[:4]
    #skipping missing dates
    if date_added is None:
        continue
    #map payment type to DB ID, defaulting to 2 if not found
    payment_method_id = payment_method_map.get(payment_type, 2)

    if isinstance(date_added, str):
        if len(date_added) > 10 and date_added[10] != ' ':
            date_added = date_added[:10] + ' ' + date_added[10:]
        try:
            parsed_datetime = datetime.strptime(date_added, '%d/%m/%Y %H:%M:%S')
            date_added = parsed_datetime.strftime('%Y-%m-%d')
            time_stamp = parsed_datetime.strftime('%H:%M:%S')
        except ValueError:
            continue
    else:
        try:
            date_added = date_added.strftime('%Y-%m-%d')
            time_stamp = date_added.strftime('%H:%M:%S')
        except AttributeError:
            continue
    #collect cleaned and transformed data for bulk insert
    sales_data.append((order_id, payment_method_id, total, date_added, time_stamp))
#load: insert or update transformed sales data into the database
cursor.executemany("""
    INSERT INTO sales (order_id, payment_method_id, total, date_added, time_stamp)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
    payment_method_id = VALUES(payment_method_id),
    total = VALUES(total),
    date_added = VALUES(date_added),
    time_stamp = VALUES(time_stamp)
""", sales_data)

connection.commit()
cursor.close()
connection.close()