import mysql.connector
import openpyxl

"""This code demonstrates a workflow for reading Excel sales data and updating a MySQL database. This is all locally on the machine."""
#con to sqldb
connection = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)
cursor = connection.cursor()
#excel load, no pandas here as we want direct conection. extraction
file_path = "xlsx/salesbyproduct.xlsx"
wb = openpyxl.load_workbook(file_path)
sheet = wb.active

#storing data temp
product_type_data = set()
product_data = []

#transformation layer
for row in sheet.iter_rows(min_row=2, values_only=True):
    if len(row) < 4:
        continue

    product_name, model, quantity_sold, total = row[:4]

    product_name = product_name.strip() if isinstance(product_name, str) else product_name
    model = model.strip() if isinstance(model, str) else model

    product_type = model
    product_type_data.add(product_type)

    product_data.append((product_name, product_type, quantity_sold, total))

#updating/loading lyaer
cursor.executemany("""
    INSERT INTO product_type (product_type)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE product_type = VALUES(product_type)
""", [(product_type,) for product_type in product_type_data])

product_type_id_map = {}
cursor.execute("SELECT product_typeid, product_type FROM product_type")
for product_typeid, product_type in cursor.fetchall():
    product_type_id_map[product_type] = product_typeid

#dictionary to sum the quantities and totals for each product
product_summary = {}

for product_name, product_type, quantity_sold, total in product_data:
    product_typeid = product_type_id_map.get(product_type)
    if product_typeid:
        key = (product_name, product_typeid)
        if key in product_summary:
            #sum quantity sold if prod exists
            product_summary[key] = (
                product_summary[key][0] + quantity_sold,
                product_summary[key][1] + total
            )
        else:
            product_summary[key] = (quantity_sold, total)

#insert summed data into table
product_inserts = [
    (product_name, product_typeid, quantity_sold, total)
    for (product_name, product_typeid), (quantity_sold, total) in product_summary.items()
]

cursor.executemany("""
    INSERT INTO product (product_name, product_typeid, quantity_sold, total)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    quantity_sold = quantity_sold + VALUES(quantity_sold),
    total = total + VALUES(total)
""", product_inserts)

connection.commit()
cursor.close()
connection.close()