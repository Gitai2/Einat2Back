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

        decoded_csv = base64.b64decode(base64_csv).decode('utf-8')
        reader = csv.DictReader(StringIO(decoded_csv))

        # SQL connection details from environment
        conn = pymssql.connect(
            server=os.environ["SQL_SERVER"],
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            database=os.environ["SQL_DATABASE"]
        )

        cursor = conn.cursor()

        processed_emails = set()

        for row in reader:
            email = row.get("Einat2Email")
            grade = row.get("Einat2Grade")
            sentence = row.get("Einat2Sentence")

            cursor.callproc("sp_InsertEinat2Raw", (email, grade, sentence, source_file))
            processed_emails.add(email)
            logging.info(f"✅ Inserted row for {email}")

        # Update totals per email
        for email in processed_emails:
            cursor.callproc("sp_UpdateEinat2Totals", (email,))
            logging.info(f"🔄 Updated total for {email}")

        conn.commit()
        conn.close()
        return func.HttpResponse("✅ CSV processed successfully", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
