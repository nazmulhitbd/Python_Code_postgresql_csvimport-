import psycopg2
import pandas as pd

# Database connection settings
db_config = {
    'dbname': 'video_game_sale',
    'user': 'postgres',
    'password': 'Admin@123',
    #'host': '192.168.2.106',  # or your database host
    'host': '10.220.25.205',  # or your database host
    'port': '5434'        # Default PostgreSQL port
}


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


# Path to your CSV file
csv_file_path = '/Users/nazmulbhuiyan/Desktop/yolo/vgchartz-2024.csv'  # Update with your actual CSV file path


# Load the CSV into a pandas DataFrame
csv_file_path = '/Users/nazmulbhuiyan/Desktop/yolo/vgchartz-2024.csv'  # Path to your CSV file
df = pd.read_csv(csv_file_path)

# Replace empty strings with None (which pandas interprets as NULL)
df['last_update'] = df['last_update'].replace('', None)
df['release_date'] = df['release_date'].replace('', None)  # Handle release_date similarly

# Save the processed DataFrame back to CSV (without the index)
cleaned_csv_file_path = 'cleaned_data.csv'
df.to_csv(cleaned_csv_file_path, index=False)

# Create a connection to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    print("Successfully connected to the PostgreSQL database.")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# Copy the cleaned CSV to PostgreSQL
try:
    with open(cleaned_csv_file_path, 'r') as f:
        next(f)  # Skip the header row
        cursor.copy_expert("COPY vgchartz FROM STDIN WITH CSV HEADER NULL AS ''", f)
    conn.commit()
    print("Data copied successfully.")
except Exception as e:
    print(f"Error copying data: {e}")
    conn.rollback()

# Close the cursor and connection
cursor.close()
conn.close()
