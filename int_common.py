import psycopg2
import pandas as pd

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "123"
DB_HOST = "localhost"
DB_PORT = "5433"

# Path to your CSV file
CSV_FILE_PATH = "./unigram_freq.csv"

# Define the table name and schema (assumes all columns are text, adjust as needed)
TABLE_NAME = "common"

def create_unigram_table_from_csv(cursor, df):
    """
    Create a table in PostgreSQL based on the CSV column names.
    Assumes all columns are text by default.
    """
    # Generate a SQL CREATE TABLE statement based on CSV columns
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    columns = 'word_id INT PRIMARY KEY, word VARCHAR(255), count BIGINT CHECK (count > 0)'
    create_table_query = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns});"
    cursor.execute(create_table_query)

def insert_data_from_csv(cursor, df):
    """
    Insert CSV data into PostgreSQL table.
    """
    # Generate a list of tuples for batch insert
    id = 1
    rows = []
    for x in df.to_numpy():
        row = (id,) + tuple(x)
        id += 1;
        rows.append(row)

    print("Rows read from file")

    columns = 'word_id, word, count'
    placeholders = ', '.join(['%s'] * 3)
    insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

    # Execute the batch insert
    cursor.executemany(insert_query, rows)

def main():
    # Read CSV into DataFrame
    df = pd.read_csv(CSV_FILE_PATH)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        # Create table if not exists
        create_unigram_table_from_csv(cursor, df)
        print(f"Table '{TABLE_NAME}' created.")

        # Insert data
        insert_data_from_csv(cursor, df)
        print(f"Data inserted into '{TABLE_NAME}' successfully.")
    except Exception as e:
        print("An error occurred:", e)
    finally:
        # Close connections
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
