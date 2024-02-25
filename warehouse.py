import logging
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

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

source_engine = create_engine(f"mysql+mysqlconnector://{source_db_config['user']}:{source_db_config['password']}@{source_db_config['host']}/{source_db_config['database']}")
target_engine = create_engine(f"mysql+mysqlconnector://{target_db_config['user']}:{target_db_config['password']}@{target_db_config['host']}/{target_db_config['database']}")

def extract_from_source(table_name):
    source_connection = mysql.connector.connect(**source_db_config)
    source_cursor = source_connection.cursor()
    data_list = []

    for table in table_name:
        if table == 'transportfact':
            query = f"SELECT * FROM datawarehouse.{table}"
        else:
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

        load_to_transportfact(target_cursor, data)

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
            category_name = row[1]

            cursor.execute("SELECT id FROM datawarehouse.dimproduct_subcategory WHERE subcategory = %s", (category_name,))
            existing_category = cursor.fetchone()

            if not existing_category:
                cursor.execute("INSERT INTO datawarehouse.dimproduct_subcategory (subcategory) VALUES (%s)", (category_name,))
                cursor.execute("SELECT LAST_INSERT_ID()") 
                category_id = cursor.fetchone()[0]
            else:
                category_id = existing_category[0]

            cursor.execute("INSERT INTO datawarehouse.dimproduct (id, code, family, category_id) VALUES (%s, %s, %s, %s)", (row[0], row[2], row[3], category_id))
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

def load_to_transportfact(cursor, data):
    for row in data:
        cursor.execute("SELECT id FROM datawarehouse.transportfact WHERE id = %s", (row[0],))
        existing_record = cursor.fetchone()

        if not existing_record:
            cursor.execute("INSERT INTO datawarehouse.transportfact (product_id, client_id, date_id, country_id, quantity, price, year) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
        else:
            logger.warning(f"Record with id {row[0]} already exists in transportfact. Skipping insertion.")

def transform_and_load_to_cube():
    target_connection = mysql.connector.connect(**target_db_config)
    target_cursor = target_connection.cursor()

    try:
        target_cursor.execute("START TRANSACTION")

        target_cursor.execute("TRUNCATE TABLE datawarehouse.cube_table")

        companies_data = extract_from_source(['companies'])
        products_data = extract_from_source(['products'])
        countries_data = extract_from_source(['countries'])
        regions_data = extract_from_source(['regions'])
        transport_fact_data = extract_from_source(['transportfact'])

        load_companies_dimension(target_cursor, companies_data)
        load_products_dimension(target_cursor, products_data)
        load_countries_dimension(target_cursor, countries_data)
        load_regions_dimension(target_cursor, regions_data)
        load_to_transportfact(target_cursor, transport_fact_data)

        target_cursor.execute("COMMIT")
        logger.info("Successfully loaded data into cube_table")

    except Exception as e:
        target_cursor.execute("ROLLBACK")
        logger.error(f"Error loading data into cube_table: {e}")

    finally:
        target_cursor.close()
        target_connection.close()

if __name__ == "__main__":
    tables = ['companies', 'companies_regions', 'countries', 'products', 'regions', 'regions_products', 'suppliers', 'transfers']

    for table in tables:
        extracted_data = extract_from_source([table])
        transform_and_load_to_target(extracted_data, table)

    transform_and_load_to_cube()
