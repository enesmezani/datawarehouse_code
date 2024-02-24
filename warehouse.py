import mysql.connector

source_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'transportation'
}

target_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'datawarehouse'
}

def extract_from_source(table_name):
    source_connection = mysql.connector.connect(**source_db_config)
    source_cursor = source_connection.cursor()
    data_list = []

    for table in table_name:
        query = f"SELECT * FROM transportation.{table}"
        source_cursor.execute(query)
        data = source_cursor.fetchall()
        data_list.extend(data)  # Use extend to add elements of data to data_list

    source_cursor.close()
    source_connection.close()

    return data_list

def transform_and_load_to_target(data, table_name):
    target_connection = mysql.connector.connect(**target_db_config)
    target_cursor = target_connection.cursor()

    try:
        target_cursor.execute("START TRANSACTION")

        # if table_name == 'companies':
        #     for row in data:
        #         target_cursor.execute("INSERT INTO datawarehouse.dimclient (id, company_name) VALUES (%s, %s)", (row[0], row[1],))
        # elif table_name == 'products':
        #     for row in data:
        #         target_cursor.execute("INSERT INTO datawarehouse.dimproduct (id, category, name, family) VALUES (%s, %s, %s, %s)", (row[0], row[3], row[2], row[1],))
        if table_name == 'countries':
            for row in data:
                country_id, country_name = row[0], row[1]

                # Join countries and regions to get region information
                source_connection = mysql.connector.connect(**source_db_config)
                source_cursor = source_connection.cursor()
                join_query = f"""
                    SELECT countries.id, countries.name, regions.name
                    FROM transportation.countries
                    LEFT JOIN transportation.regions ON countries.id = regions.country_id
                    WHERE countries.id = {country_id}
                """
                source_cursor.execute(join_query)
                result = source_cursor.fetchone()
                source_cursor.close()

                # Extract values from the joined result
                region_id, region_name = result[0], result[2]

                target_cursor.execute("INSERT INTO datawarehouse.dimcountry (id, name, region) VALUES (%s, %s, %s)", (country_id, country_name, region_name))

        target_cursor.execute("COMMIT")

    except Exception as e:
        target_cursor.execute("ROLLBACK")
        print(f"Error: {e}")

    finally:
        target_cursor.close()
        target_connection.close()

if __name__ == "__main__":
    tables = ['companies', 'companies_regions', 'countries', 'products', 'regions', 'regions_products', 'suppliers', 'transfers']

    # Extract data from each table in the source database
    for table in tables:
        extracted_data = extract_from_source([table])

        # Transform and load data into the corresponding dimension
        transform_and_load_to_target(extracted_data, table)
