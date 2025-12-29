from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import pyodbc
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook

def connect_to_iris():
    """Connexion à IRIS via ODBC (Airflow connection : iris_odbc)."""
    conn = BaseHook.get_connection("iris_odbc")
    dsn = conn.host

    connection = pyodbc.connect(
        f"DSN={dsn};UID={conn.login};PWD={conn.password}"
    )
    connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    connection.setencoding(encoding='utf-8')

    return connection


def get_postgres_hook(conn_id=None):
    """Récupère un hook PostgreSQL via Airflow Variable (ou fallback postgres_test)."""
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)


def get_oncopole_hook(conn_id=None):
    """
    Récupère une connexion PostgreSQL Oncopole depuis Airflow Connection.
    Si aucun conn_id n'est fourni, prend 'oncopole_test' par défaut.
    """
    if not conn_id:
        conn_id = "postgres_test"

    conn = BaseHook.get_connection(conn_id)

    connection = psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    return connection
