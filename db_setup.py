import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_NAME = os.getenv('LEADGEN_DB', 'leadgen')
DB_USER = os.getenv('LEADGEN_DB_USER', 'postgres')
DB_PASSWORD = os.getenv('LEADGEN_DB_PASSWORD', '')
DB_HOST = os.getenv('LEADGEN_DB_HOST', 'localhost')
DB_PORT = os.getenv('LEADGEN_DB_PORT', '5432')

# SQL statements
CREATE_DB_SQL = f"CREATE DATABASE {DB_NAME};"

CREATE_CONTACTS_TABLE = """
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    title TEXT,
    company_name TEXT,
    company_website TEXT,
    company_industry TEXT,
    company_size TEXT,
    company_location TEXT,
    zi_enriched BOOLEAN DEFAULT FALSE,
    company_enriched BOOLEAN DEFAULT FALSE,
    enrichment_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS enrichment_errors (
    id SERIAL PRIMARY KEY,
    contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
    error TEXT,
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_OUTPUT_LEADS_TABLE = """
CREATE TABLE IF NOT EXISTS output_leads (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    contact_phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);",
    "CREATE INDEX IF NOT EXISTS idx_output_leads_email ON output_leads(email);"
]

def create_database():
    # Connect to default database to create the target DB
    con = psycopg2.connect(dbname='postgres', user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    # Check if DB exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
    exists = cur.fetchone()
    if not exists:
        print(f"Creating database '{DB_NAME}'...")
        cur.execute(CREATE_DB_SQL)
    else:
        print(f"Database '{DB_NAME}' already exists.")
    cur.close()
    con.close()

def create_tables():
    con = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    cur = con.cursor()
    print("Creating tables if not exist...")
    cur.execute(CREATE_CONTACTS_TABLE)
    cur.execute(CREATE_ERRORS_TABLE)
    cur.execute(CREATE_OUTPUT_LEADS_TABLE)
    for idx_sql in CREATE_INDEXES:
        cur.execute(idx_sql)
    con.commit()
    cur.close()
    con.close()
    print("All tables and indexes are set up.")

def main():
    try:
        create_database()
        create_tables()
        print("Database setup complete.")
    except Exception as e:
        print(f"Error during DB setup: {e}")

if __name__ == "__main__":
    main() 