import psycopg2


def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "9999"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    import os
    import csv

    def infer_data_type(value):
        try:
            int(value)
            return 'INTEGER'
        except ValueError:
            try:
                float(value)
                return 'FLOAT'
            except ValueError:
                return 'TEXT'

    def generate_sql_create(folder_path, sample_size=100):
        sql_statements = []

        # Iterate over the files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                table_name = os.path.splitext(filename)[0]
                csv_file_path = os.path.join(folder_path, filename)

                # Read a sample of rows from the CSV file
                sample_rows = []
                with open(csv_file_path, 'r') as file:
                    csv_reader = csv.reader(file)
                    header = next(csv_reader)

                    for _ in range(sample_size):
                        try:
                            row = next(csv_reader)
                            sample_rows.append(row)
                        except StopIteration:
                            break

                # Analyze the data types based on the sample rows
                column_types = []
                for i in range(len(header)):
                    values = [row[i] for row in sample_rows]
                    column_type = max(set(map(infer_data_type, values)), key=values.count)
                    column_types.append(column_type)
                    print(1)
                    

                # Generate the column definitions
                column_definitions = []
                primary_key_columns = []

                for column_name, column_type in zip(header, column_types):
                    column_definitions.append(f"{column_name} {column_type}")

                    # Check if the column name is 'id' to define it as the primary key
                    if column_name.lower() == 'id':
                        primary_key_columns.append(column_name)

                # Create the CREATE TABLE statement
                create_statement = f"CREATE TABLE {table_name} (  {', '.join(column_definitions)}"

                # Add primary key constraint
                if primary_key_columns:
                    create_statement += f",\n  PRIMARY KEY ({', '.join(primary_key_columns)})"

                create_statement += ");"

                # Append the CREATE statement to the list
                sql_statements.append(create_statement)
        print(sql_statements)

        return sql_statements

    # Specify the folder path containing the CSV files
    folder_path = 'data'  # Replace with the actual folder path

    # Generate the SQL CREATE statements
    sql_create_statements = generate_sql_create(folder_path)
    # Create a cursor object
    cursor = conn.cursor()

    # Execute the SQL statements
    for statement in sql_create_statements:
        cursor.execute(statement)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

if __name__ == "_main_":
    main()
    print (1)