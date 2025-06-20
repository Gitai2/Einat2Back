# Function_ProcessEinat2File/__init__.py
import logging
import azure.functions as func
import pymssql
import os
import csv
from azure.storage.blob import BlobServiceClient
from io import StringIO


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("📦 Einat2 CSV Function triggered")

    try:
        req_body = req.get_json()
        blob_name = req_body.get("blobName")
        if not blob_name:
            return func.HttpResponse("Missing 'blobName' in body", status_code=400)

        logging.info(f"📁 Blob to load: {blob_name}")

        # Connect to Blob
        blob_service_client = BlobServiceClient.from_connection_string(os.environ["AzureWebJobsStorage"])
        container_client = blob_service_client.get_container_client("einat2uploads")
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall().decode("utf-8")

        # Parse CSV
        reader = csv.DictReader(StringIO(blob_data))
        records = list(reader)

        logging.info(f"📄 Parsed {len(records)} rows")

        # Connect to SQL
        conn = pymssql.connect(
            server=os.environ["SQL_SERVER"],
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            database=os.environ["SQL_DATABASE"]
        )
        cursor = conn.cursor()

        emails = set()

        for row in records:
            email = row.get("Einat2Email")
            grade = row.get("Einat2Grade")
            sentence = row.get("Einat2Sentence")

            if not all([email, grade, sentence]):
                logging.warning(f"⚠️ Skipping incomplete row: {row}")
                continue

            cursor.execute(
                """
                INSERT INTO Einat2Raw (Einat2Email, Einat2Grade, Einat2Sentence)
                VALUES (%s, %s, %s)
                """,
                (email, grade, sentence)
            )
            emails.add(email)

        for email in emails:
            try:
                cursor.callproc("sp_UpdateEinat2Totals", (email,))
                logging.info(f"🔁 Totals refreshed for {email}")
            except Exception as sp_err:
                logging.error(f"❌ Failed totals update for {email}: {sp_err}")

        conn.commit()
        conn.close()

        return func.HttpResponse("✅ CSV processed and totals updated", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
