import psycopg2
import pandas as pd
# Initialize connection variable
#connection = None

# Step 1: Connect to the PostgreSQL database
try:
    connection = psycopg2.connect(
        user="postgres",  # Replace with your PostgreSQL username
        password="Admin@123",  # Replace with your PostgreSQL password
        host="192.168.2.106",  # Replace with your Docker container's host (or use the container's name if inside Docker)
        port="5433",  # PostgreSQL default port (adjust if necessary)
        database="video_game_sale"  # Replace with your PostgreSQL database name
    )
    cursor = connection.cursor()

    # Step 2: Create the table (if it doesn't exist)
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
    cursor.execute(create_table_query)
    connection.commit()
    print("Table 'vgchartz' created successfully (if not exists).")

    # Step 3: Read the CSV file
    # Replace '/Users/nazmulbhuiyan/Desktop/yolo/vgchartz.csv' with your actual CSV file path
    df = pd.read_csv('/Users/nazmulbhuiyan/Desktop/yolo/vgchartz-2024.csv')

    # Step 4: Insert CSV data into the table
    for row in df.itertuples():
        insert_query = f'''
        INSERT INTO vgchartz (img, title, console, genre, publisher, developer, critic_score, total_sales, na_sales, jp_sales, pal_sales, other_sales, release_date, last_update)
        VALUES (
        '{row.img}', 
        '{row.title}',
        '{row.console}',
        '{row.genre}',
        '{row.publisher}',
        '{row.developer}',
        '{row.critic_score}',
        '{row.total_sales}',
        '{row.na_sales}',
        '{row.jp_sales}',
        '{row.pal_sales}',
        '{row.other_sales}',
        '{row.release_date}',
        '{row.last_update}'
        );
      
        '''
        cursor.execute(insert_query)

    connection.commit()
    print(f"Data inserted successfully into 'vgchartz' table from CSV file.")

except Exception as error:
    print(f"Error while interacting with PostgreSQL: {error}")

finally:
    # Step 5: Close the database connection
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection closed.")
