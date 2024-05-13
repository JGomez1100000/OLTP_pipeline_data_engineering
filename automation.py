# Import libraries required for connecting to mysql
import mysql.connector

# Import libraries required for connecting PostgreSql
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='12345678',host='localhost',database='sales')
# create cursor
cursor_mysql = connection.cursor()

# Connect to DB2 or PostgreSql
# connection details
dsn_hostname = 'localhost'
dsn_user='postgres'
dsn_pwd ='123456'
dsn_port ="5432" 
dsn_database ="postgres"

# create connection
conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)
#Create a cursor object
cursor_psql = conn.cursor()

# Find out the last rowid from PostgreSql data warehouse

def get_last_rowid():
    SQL = "SELECT MAX(rowid) FROM sales_data"
    cursor_psql.execute(SQL)
    last_rowid = cursor_psql.fetchone()[0]
    return last_rowid

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(last_rowid):
    SQL = "SELECT * FROM sales_data WHERE rowid > %s"
    cursor_mysql.execute(SQL, (last_rowid,))
    records = cursor_mysql.fetchall()
    return records


new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in PostgreSql.

def insert_records(records):
    SQL = "INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (%s, %s, %s, %s)"
    
    for record in records:
        cursor_psql.execute(SQL, record)
        
    conn.commit()
        
    print("Registros insertados exitosamente.")


insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
connection.close()
# disconnect from PostgreSql data warehouse 
conn.close()

# End of program
print("End of program")