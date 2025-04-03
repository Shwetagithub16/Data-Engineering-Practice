import psycopg2
import csv
import os  # Added to check if files exist

#Taks:
#Ingest data from csv files to Postgres tables.

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"

    def execute_sql_statements(conn):
        """ Executes SQL table creation statements directly in the script. """
        sql_statements = [
            """
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                product_code INT NOT NULL,
                product_description TEXT NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                first_name VARCHAR(50) NOT NULL,
                last_name VARCHAR(50) NOT NULL,
                address_1 VARCHAR(255) NOT NULL,
                address_2 VARCHAR(255),
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50) NOT NULL,
                zip_code VARCHAR(10) NOT NULL,
                join_date DATE NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                transaction_date DATE NOT NULL,
                product_id INT NOT NULL,
                product_code INT NOT NULL,
                product_description VARCHAR(255) NOT NULL,
                quantity INT NOT NULL CHECK (quantity > 0),
                account_id INT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
                FOREIGN KEY (account_id) REFERENCES customers(customer_id) ON DELETE CASCADE
            );
            """
        ]

        try:
            with conn.cursor() as cur:
                for sql in sql_statements:
                    cur.execute(sql)
                conn.commit()
                print("Tables created successfully.")
        except Exception as e:
            print(f"Error executing SQL statements: {e}")

    def load_csv_to_table(csv_file, table_name, conn):
        """ Loads data from CSV files into the respective tables. """
        try:
            if not os.path.exists(csv_file):
                print(f"Error: CSV file '{csv_file}' not found.")
                return

            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                headers = next(reader)  # Extract headers from CSV

                with conn.cursor() as cur:
                    columns = ", ".join(headers)
                    placeholders = ", ".join(["%s"] * len(headers))
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                    for row in reader:
                        cur.execute(query, row)

                    conn.commit()
                    print(f"Data from {csv_file} inserted into {table_name} successfully.")

        except Exception as e:
            print(f"Error loading {csv_file} into {table_name}: {e}")

    try:
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)

        # Execute SQL table creation
        execute_sql_statements(conn)

        # Load data from CSVs into tables
        csv_table_mapping = {
            "products.csv": "products",
            "accounts.csv": "customers",
            "transactions.csv": "transactions"
        }

        for csv_file, table_name in csv_table_mapping.items():
            load_csv_to_table(csv_file, table_name, conn)

        conn.close()
        print("Database connection closed.")

    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    main()
