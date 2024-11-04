from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from werkzeug.serving import make_server
import threading

INFLUXDB_TOKEN = "1T_6rLw7sMXiCNWK8bU6cLYNwSihdOO-dee210OYkzFD8DQaIScjUKcK0WXTrBZLNR8HkIqxZU6bzvPEwrHxPw=="

token = INFLUXDB_TOKEN
org = "test"
bucket = "timeseriesData"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

app = Flask(__name__)

@app.route("/fetch_data", methods=["GET"])
def fetch_data():
    start = request.args.get("start")
    stop = request.args.get("stop")

    # Validate that start and stop parameters are provided
    if not start or not stop:
        return jsonify({"error": "Please provide both 'start' and 'stop' parameters in ISO 8601 format"}), 400

    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {start}, stop: {stop})
    |> filter(fn: (r) => r["_measurement"] == "energy_data")
    |> filter(fn: (r) => r["_field"] == "RH_2" or r["_field"] == "RH_3")
    |> filter(fn: (r) => r["deviceId"] == "1155")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    '''
    try:
        result = query_api.query(org=org, query=query)
        results = []

        for table in result:
            for record in table.records:
                results.append({
                    "time": record.get_time(),
                    "field": record.get_field(),
                    "value": record.get_value()
                })

        print("Fetched data:", results)
        return jsonify(results)
    except Exception as e:
        print("Error fetching data:", str(e))
        return jsonify({"error": "Error fetching data"}), 500

class FlaskThread(threading.Thread):
    def __init__(self, app):
        super().__init__()
        self.srv = make_server('127.0.0.1', 5000, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        print("Starting server")
        self.srv.serve_forever()

    def shutdown(self):
        print("Shutting down server")
        self.srv.shutdown()

# Start the Flask server in a separate thread
flask_thread = FlaskThread(app)
flask_thread.start()

# To stop the server, you would call `flask_thread.shutdown()`
