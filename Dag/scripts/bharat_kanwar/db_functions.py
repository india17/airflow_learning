import psycopg2
import pandas as pd
import logging

# from airflow.models import Variable

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# HOST = os.environ.get("HOST")  # Variable.get("HOST")
# DB_NAME = os.environ.get("DB_NAME")  # Variable.get("DB_NAME")
# USER = os.environ.get("USER")  # Variable.get("USER")
# PASSWORD = os.environ.get("PASSWORD")  # Variable.get("PASSWORD")


# Establish PostgreSQL connection
def get_pg_connection(postgressql_congif):
    try:
        conn = psycopg2.connect(**postgressql_congif)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def get_last_run_date(conn, table_name):
    try:
        query = f"SELECT last_run_date FROM etl_last_run_metadata WHERE table_name = '{table_name}'"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error fetching last run date: {e}")
        raise


def fetch_new_data(conn, table_name, last_run_date):
    try:
        query = f"""
        SELECT * FROM {table_name} 
        WHERE created_at > %s OR updated_at > %s
        """
        df = pd.read_sql(query, conn, params=(last_run_date, last_run_date))
        return df
    except Exception as e:
        logger.error(f"Error fetching new data from {table_name}: {e}")
        raise


def update_last_run_date(conn, updated_last_run_date, table_name):
    try:
        query = f"""
        UPDATE etl_last_run_metadata
        SET last_run_date = %s
        WHERE table_name = %s
        """
        cursor = conn.cursor()
        cursor.execute(query, (updated_last_run_date, table_name))
        conn.commit()
        logger.info(f"Updated last run date for {table_name}.")
    except Exception as e:
        logger.error(f"Error updating last run date: {e}")
        conn.rollback()
        raise


def insert_last_run_date(conn, updated_last_run_date, table_name):
    try:
        query = f"""
         INSERT INTO etl_last_run_metadata (table_name, last_run_date)
        VALUES (%s, %s)
        """
        cursor = conn.cursor()
        cursor.execute(query, (table_name, updated_last_run_date))
        conn.commit()
        logger.info(f"Inserted last run date for {table_name}.")
    except Exception as e:
        logger.error(f"Error updating last run date: {e}")
        conn.rollback()
        raise
