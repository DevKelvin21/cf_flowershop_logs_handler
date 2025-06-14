import os
from datetime import datetime
import pytz
from flask import Flask, request, jsonify
from google.cloud import bigquery
import functions_framework

app = Flask(__name__)

# Set your BigQuery table details here
BQ_PROJECT = os.getenv("BQ_PROJECT", "your_project_id")
BQ_DATASET = os.getenv("BQ_DATASET", "your_dataset_id")
BQ_TABLE = os.getenv("BQ_TABLE", "your_table_name")

@app.route("/log_operation", methods=["POST"])
def log_operation():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Missing JSON payload"}), 400

    operation_type = data.get("operation_type")
    user_name = data.get("user_name")
    message = data.get("message")

    if not all([operation_type, user_name, message]):
        return jsonify({"error": "Missing required fields"}), 400

    tz = pytz.timezone("America/El_Salvador")
    timestamp = datetime.now(tz).isoformat()

    row = {
        "timestamp": timestamp,
        "user_id": None,
        "chat_id": None,
        "operation_type": operation_type,
        "message_content": message,
        "user_name": user_name,
        "transaction_id": None
    }

    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        return jsonify({"error": str(errors)}), 500
    return jsonify({"status": "success"}), 200

# For Cloud Functions (Gen 2) entry point
@functions_framework.http
def main(request):
    if request.path == "/log_operation":
        return log_operation()
    return ("Not Found", 404)