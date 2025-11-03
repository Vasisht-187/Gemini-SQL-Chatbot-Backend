import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "127.0.0.1"),
    "port": int(os.environ.get("DB_PORT", 3306)),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "database": os.environ.get("DB_NAME"),
    "autocommit": True,
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def run_select(sql, params=None, limit=200):
    """
    Execute a SELECT safely. Returns list of dict rows.
    Enforces a LIMIT if none present, to avoid huge results.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(dictionary=True)

        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            sql = f"{sql.strip()} LIMIT {limit}"

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    finally:
        cursor.close()
        conn.close()

