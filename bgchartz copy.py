import pandas as pd
import psycopg2

# Database connection settings
db_config = {
    'dbname': 'video_game_sale',
    'user': 'postgres',
    'password': 'Admin@123',
    'host': '192.168.2.106',  # or the IP address of your PostgreSQL instance
    'port': '5433'  # Default port for PostgreSQL
}

# Path to your CSV file
csv_file = '/Users/nazmulbhuiyan/Desktop/yolo/vgchartz-2024.csv'  # Update this with the actual path to your CSV

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Successfully connected to the PostgreSQL database.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Define the SQL to create the table
create_table_query = '''
CREATE TABLE IF NOT EXISTS vgchartz (
    img TEXT,
    title VARCHAR(255),
    console VARCHAR(100),
    genre VARCHAR(100),
    publisher VARCHAR(255),
    developer VARCHAR(255),
    critic_score FLOAT,
    total_sales FLOAT,
    na_sales FLOAT,
    jp_sales FLOAT,
    pal_sales FLOAT,
    other_sales FLOAT,
    release_date DATE,
    last_update DATE
);
'''

# Create the table
try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")
    conn.rollback()

# Load the CSV data into a DataFrame
try:
    df = pd.read_csv(csv_file)
    print("Data successfully loaded from CSV.")
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit()

# Insert data into the table
insert_query = '''
INSERT INTO vgchartz (img, title, console, genre, publisher, developer, critic_score, total_sales, na_sales, jp_sales, pal_sales, other_sales, release_date, last_update)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Loop over the DataFrame and insert row by row
try:
    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    conn.commit()
    print("Data inserted successfully.")
except Exception as e:
    print(f"Error during data insertion: {e}")
    conn.rollback()

# Close the connection
cursor.close()
conn.close()
print("Connection closed.")
