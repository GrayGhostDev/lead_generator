from flask import Flask, request, jsonify
import logging
from dotenv import load_dotenv
import psycopg2
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('LEADGEN_DB', 'leadgen'),
        user=os.getenv('LEADGEN_DB_USER', 'postgres'),
        password=os.getenv('LEADGEN_DB_PASSWORD', ''),
        host=os.getenv('LEADGEN_DB_HOST', 'localhost'),
        port=os.getenv('LEADGEN_DB_PORT', '5432')
    )

@app.route('/rocketreach/webhook', methods=['POST'])
def rocketreach_webhook():
    # Get the JSON data sent by RocketReach
    webhook_data = request.json
    logger.info(f"Received webhook data: {webhook_data}")
    
    # Process the data as needed
    # Example: Validate required fields and log or store the data
    required_fields = ["name", "email"]
    missing_fields = [field for field in required_fields if field not in webhook_data or not webhook_data[field]]
    if missing_fields:
        logger.error(f"Missing required fields in webhook: {missing_fields}")
        return jsonify({"status": "error", "message": f"Missing fields: {', '.join(missing_fields)}"}), 400
    # Example: Extract and log details
    name = webhook_data.get("name")
    email = webhook_data.get("email")
    company = webhook_data.get("company", "")
    phone = webhook_data.get("phone", "")

    # Split name into first and last for the contacts table
    first_name, last_name = (name.split(" ", 1) + [""])[:2]

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO contacts (first_name, last_name, email, phone, company_name)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (first_name, last_name, email, phone, company)
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Inserted contact into database: {name} ({email})")
    except Exception as e:
        logger.error(f"Database insert error: {e}")
        return jsonify({"status": "error", "message": "Database error"}), 500

    logger.info(f"Processed contact: Name={name}, Email={email}, Company={company}, Phone={phone}")
    return jsonify({"status": "success", "message": "Webhook data processed and stored."}), 200
    