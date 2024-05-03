#Crear tablas de DB

import psycopg2
from psycopg2 import sql

# Connect to the RDS PostgreSQL database
conn = psycopg2.connect(
    database = "db-tp-lmi",
    user = "user_lmi",
    password = "basededatoslmi",
    host = "db-tp-lmi.cjuseewm8uut.us-east-1.rds.amazonaws.com",
    port = "5432" )

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# SQL command to create Top_CTR table
create_top_ctr_table = """
CREATE TABLE IF NOT EXISTS Top_CTR (
    advertiser_id VARCHAR(255),
    product_id VARCHAR(255),
    CTR FLOAT,
    date VARCHAR(255)
);
"""

# SQL command to create Top_views table
create_top_views_table = """
CREATE TABLE IF NOT EXISTS Top_views (
    advertiser_id VARCHAR(255),
    product_id VARCHAR(255),
    views INT,
    date VARCHAR(255)
);
"""



# Execute the SQL commands to create the tables
cursor.execute(create_top_ctr_table)
cursor.execute(create_top_views_table)

#test
cursor.execute("""SELECT * FROM Top_views;""")

print(cursor.fetchall())

# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()
