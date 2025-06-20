# Function_ProcessEinat2File/__init__.py
import logging
import azure.functions as func
import pymssql
import csv
from io import StringIO
import os
import base64

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("🚀 Function started")

    try:
        body = req.get_json()
        base64_csv = body.get("csvBase64")
        source_file = body.get("sourceFile", "UnknownFile.csv")

        if not base64_csv:
            logging.error("❌ No base64 CSV provided in request body")
            return func.HttpResponse("Missing 'csvBase64' in request", status_code=400)

        try:
            decoded_csv = base64.b64decode(base64_csv).decode('utf-8')
        except Exception as decode_error:
            logging.error(f"❌ Failed to decode CSV: {decode_error}")
            return func.HttpResponse("Invalid base64-encoded CSV", status_code=400)

        reader = csv.DictReader(StringIO(decoded_csv))
        logging.warning(f"📄 HEADERS: {reader.fieldnames}")

        # SQL connection
        try:
            conn = pymssql.connect(
                server=os.environ["SQL_SERVER"],
                user=os.environ["SQL_USER"],
                password=os.environ["SQL_PASSWORD"],
                database=os.environ["SQL_DATABASE"]
            )
            cursor = conn.cursor()
        except Exception as db_err:
            logging.error(f"❌ DB Connection failed: {db_err}")
            return func.HttpResponse("Database connection failed", status_code=500)

        processed_emails = set()

        row_count = 0
        for row in reader:
            logging.warning(f"🔍 ROW: {row}")
            email = row.get("Einat2Email") or row.get("\ufeffEinat2Email")
            grade = row.get("Einat2Grade")
            sentence = row.get("Einat2Sentence")

            if not email:
                logging.warning("⚠️ Missing email in row, skipping")
                continue

            try:
                cursor.callproc("sp_InsertEinat2Raw", (email, grade, sentence, source_file))
                processed_emails.add(email)
                row_count += 1
                logging.info(f"✅ Inserted row {row_count} for {email}")
            except Exception as insert_error:
                logging.error(f"❌ Failed to insert row: {insert_error}")

        logging.warning(f"📊 Total inserted rows: {row_count}")

        for email in processed_emails:
            try:
                cursor.callproc("sp_UpdateEinat2Totals", (email,))
                logging.info(f"🔄 Totals updated for {email}")
            except Exception as update_error:
                logging.error(f"⚠️ Failed to update totals for {email}: {update_error}")

        conn.commit()
        conn.close()

        return func.HttpResponse("✅ CSV processed successfully", status_code=200)

    except Exception as e:
        logging.error(f"❌ Top-level error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
