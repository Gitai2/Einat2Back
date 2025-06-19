import logging
import azure.functions as func
import pymssql
import os
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("🛠️ Update function triggered")

    try:
        body = req.get_json()

        row_id = body.get("RowID")
        new_grade = body.get("NewGrade")
        new_sentence = body.get("NewSentence")

        if not all([row_id, new_grade, new_sentence]):
            return func.HttpResponse("Missing required field", status_code=400)

        # SQL connection
        conn = pymssql.connect(
            server=os.environ["SQL_SERVER"],
            user=os.environ["SQL_USER"],
            password=os.environ["SQL_PASSWORD"],
            database=os.environ["SQL_DATABASE"]
        )
        cursor = conn.cursor()

        # Update row
        cursor.execute(
            "UPDATE Einat2Raw SET Einat2Grade=%s, Einat2Sentence=%s WHERE ID=%s",
            (new_grade, new_sentence, row_id)
        )

        # Get email for updated row
        cursor.execute("SELECT Einat2Email FROM Einat2Raw WHERE ID=%s", (row_id,))
        result = cursor.fetchone()
        if not result:
            return func.HttpResponse("Row not found", status_code=404)

        email = result[0]
        cursor.callproc("sp_UpdateEinat2Totals", (email,))

        conn.commit()
        conn.close()

        return func.HttpResponse("✅ Row updated and totals refreshed", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
