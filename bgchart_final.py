import psycopg2
import pandas as pd
import os

# Database connection settings
db_config = {
    'dbname': 'video_game_sale',
    'user': 'postgres',
    'password': 'Admin@123',
    'host': '192.168.2.106',  # or your database host
    'port': '5433'        # Default PostgreSQL port
}

# Path to your CSV file
csv_file_path = '/Users/nazmulbhuiyan/Desktop/yolo/vgchartz-2024.csv'  # Update with your actual CSV file path

# Create a connection to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Successfully connected to the PostgreSQL database.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Ensure the table exists (create it if not)
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
try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")
    conn.rollback()

# Copy data from CSV to PostgreSQL table
try:
    # Using the COPY command
    with open(csv_file_path, 'r') as f:
        next(f)  # Skip the header row
        cursor.copy_from(f, 'vgchartz', sep=',', columns=(
            'img', 'title', 'console', 'genre', 'publisher', 
            'developer', 'critic_score', 'total_sales', 'na_sales', 
            'jp_sales', 'pal_sales', 'other_sales', 'release_date', 
            'last_update'))
    conn.commit()
    print("Data copied successfully from CSV to PostgreSQL table.")
except Exception as e:
    print(f"Error copying data: {e}")
    conn.rollback()

# Close the cursor and connection
cursor.close()
conn.close()
print("Connection closed.")

