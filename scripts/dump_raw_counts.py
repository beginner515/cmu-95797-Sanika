import duckdb
# Connect to the DuckDB database
connection = duckdb.connect('main.db')

# Fetching the table names dynamically
query_tables = "SELECT table_name FROM information_schema.tables;"
tables = [row[0] for row in connection.execute(query_tables).fetchall()]

# Opening the raw_counts.txt file in write mode to write the table names and row counts
with open('answers/raw_counts.txt', 'w') as file:
# Fetching the row count for each table and saving to the file
    for t in tables:
        query = f"SELECT COUNT(*) FROM {t};"
        result = connection.execute(query).fetchall()
        row_count = result[0][0]
        file.write(f"Table Name: {t}, Row count: {row_count}\n")

# Closing the connection
connection.close()