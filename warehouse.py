import logging
import mysql.connector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

        if table_name == 'companies':
            load_companies_dimension(target_cursor, data)
        elif table_name == 'products':
            load_products_dimension(target_cursor, data)
        elif table_name == 'countries':
            load_countries_dimension(target_cursor, data)
        elif table_name == 'regions':
            load_regions_dimension(target_cursor, data)

        target_cursor.execute("COMMIT")
        logger.info(f"Successfully loaded data for table {table_name}")

    except Exception as e:
        target_cursor.execute("ROLLBACK")
        logger.error(f"Error loading data for table {table_name}: {e}")

    finally:
        target_cursor.close()
        target_connection.close()

def load_companies_dimension(cursor, data):
    for row in data:
        cursor.execute("SELECT id FROM datawarehouse.dimclient WHERE id = %s", (row[0],))
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute("INSERT INTO datawarehouse.dimclient (id, company_name) VALUES (%s, %s)", (row[0], row[1],))
            if row[1] == 'Company':
                company_type = 'electric_company'
                industry_id = row[0]
            elif row[1] == 'Easttest':
                company_type = 'mechanic_company'
                industry_id = row[0]
            elif row[1] == 'test company 2':
                company_type = 'industry'
                industry_id = row[0]
            cursor.execute("INSERT INTO datawarehouse.dimclient_companytype (company_type, industry_id) VALUES (%s, %s)", (company_type, industry_id,))   
        else:
            logger.warning(f"Record with id {row[0]} already exists in dimclient. Skipping insertion.")

def load_products_dimension(cursor, data):
    for row in data:
        cursor.execute("SELECT id FROM datawarehouse.dimproduct WHERE id = %s", (row[0],))
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute("INSERT INTO datawarehouse.dimproduct (id, category, code, family) VALUES (%s, %s, %s, %s)", (row[0], row[3], row[2], row[1]))

            category_name = row[3]

            cursor.execute("SELECT id FROM datawarehouse.dimproduct_subcategory WHERE subcategory = %s", (category_name,))
            existing_category = cursor.fetchone()

            if not existing_category:
                cursor.execute("INSERT INTO datawarehouse.dimproduct_subcategory (subcategory) VALUES (%s)", (category_name,))

            cursor.execute("SELECT id FROM datawarehouse.dimproduct_subcategory WHERE subcategory = %s", (category_name,))
            category_id = cursor.fetchone()[0]

            cursor.execute("UPDATE datawarehouse.dimproduct SET category_id = %s WHERE id = %s", (category_id, row[0]))
        else:
            logger.warning(f"Record with id {row[0]} already exists in dimproduct. Skipping insertion.")

def load_countries_dimension(cursor, data):
    for row in data:
        cursor.execute("SELECT id FROM datawarehouse.dimcountry WHERE id = %s", (row[0],))
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute("INSERT INTO datawarehouse.dimcountry (id, name, code) VALUES (%s, %s, %s)", (row[0], row[1], row[2],))
        else:
            logger.warning(f"Record with id {row[0]} already exists in dimcountry. Skipping insertion.")

def load_regions_dimension(cursor, data):
    for row in data:
        cursor.execute("SELECT id FROM datawarehouse.dimcountry_subregion WHERE id = %s", (row[0],))
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute("INSERT INTO datawarehouse.dimcountry_subregion (id, subregion, country_id) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
        else:
            logger.warning(f"Record with id {row[0]} already exists in dimcountry_subregion. Skipping insertion.")

if __name__ == "__main__":
    tables = ['companies', 'companies_regions', 'countries', 'products', 'regions', 'regions_products', 'suppliers', 'transfers']

    for table in tables:
        extracted_data = extract_from_source([table])

        transform_and_load_to_target(extracted_data, table)
