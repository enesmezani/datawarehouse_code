import mysql.connector
import datetime  # Import datetime module

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
        data_list.extend(data) 

    source_cursor.close()
    source_connection.close()

    return data_list

def transform_and_load_to_target(data, table_name):
    target_connection = mysql.connector.connect(**target_db_config)
    target_cursor = target_connection.cursor()

    try:
        target_cursor.execute("START TRANSACTION")

        for row in data:
            if table_name == 'companies':
                target_cursor.execute("INSERT INTO datawarehouse.dimclient (id, company_name) VALUES (%s, %s)", (row[0], row[1],))
                if row[1] == 'Company':
                    company_type = 'electric_company'
                    industry_id = row[0]
                elif row[1] == 'Easttest':
                    company_type = 'mechanic_company'
                    industry_id = row[0]
                elif row[1] == 'test company 2':
                    company_type = 'industry'
                    industry_id = row[0]
                target_cursor.execute("INSERT INTO datawarehouse.dimclient_companytype (company_type, industry_id) VALUES (%s, %s)", (company_type,industry_id,))      
            elif table_name == 'products':
                target_cursor.execute("INSERT INTO datawarehouse.dimproduct (id, category, code, family) VALUES (%s, %s, %s, %s)", (row[0], row[3], row[2], row[1]))

                category_name = row[3]

                target_cursor.execute("SELECT id FROM datawarehouse.dimproduct_subcategory WHERE subcategory = %s", (category_name,))
                existing_category = target_cursor.fetchone()

                if not existing_category:
                    target_cursor.execute("INSERT INTO datawarehouse.dimproduct_subcategory (subcategory) VALUES (%s)", (category_name,))

                target_cursor.execute("SELECT id FROM datawarehouse.dimproduct_subcategory WHERE subcategory = %s", (category_name,))
                category_id = target_cursor.fetchone()[0]

                target_cursor.execute("UPDATE datawarehouse.dimproduct SET category_id = %s WHERE id = %s", (category_id, row[0]))
            elif table_name == 'countries':
                target_cursor.execute("INSERT INTO datawarehouse.dimcountry (id, name, code) VALUES (%s, %s, %s)", (row[0], row[1], row[2],))
            elif table_name =='regions':
                target_cursor.execute("INSERT INTO datawarehouse.dimcountry_subregion (id, subregion, country_id) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
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
