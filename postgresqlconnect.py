import psycopg2

# connection details

dsn_hostname = 'localhost'
dsn_user='postgres'
dsn_pwd ='123456'
dsn_port ='5432'
dsn_database ='postgres'


# create connection

conn = psycopg2.connect(
   database=dsn_database, 
   user=dsn_user,
   password=dsn_pwd,
   host=dsn_hostname, 
   port= dsn_port
)

#Create a cursor object

cursor = conn.cursor()

# create table
SQL = """CREATE TABLE IF NOT EXISTS products(rowid INTEGER PRIMARY KEY NOT NULL,product varchar(255) NOT NULL,category varchar(255) NOT NULL)"""

# Execute the SQL statement
cursor.execute(SQL)

print("Table created")

# insert data

cursor.execute("INSERT INTO  products(rowid,product,category) VALUES(1,'Television','Electronics')");

cursor.execute("INSERT INTO  products(rowid,product,category) VALUES(2,'Laptop','Electronics')");

cursor.execute("INSERT INTO products(rowid,product,category) VALUES(3,'Mobile','Electronics')");

conn.commit()

# insert list of Records

list_ofrecords =[(5,'Mobile','Electronics'),(6,'Mobile','Electronics')]

cursor = conn.cursor()

for row in list_ofrecords:
  
   SQL="INSERT INTO products(rowid,product,category) values(%s,%s,%s)" 
   cursor.execute(SQL,row);
   conn.commit()

# query data

cursor.execute('SELECT * from products;')
rows = cursor.fetchall()
conn.commit()
conn.close()
for row in rows:
    print(row)


