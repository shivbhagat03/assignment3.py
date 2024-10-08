from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from werkzeug.serving import make_server
import threading

INFLUXDB_TOKEN = "ng00FvBO_y9QMljRt8b524Kg2fAfubNaz4XM171H8qmF6o6Dr9L_4cYPuIt4NPAtRlOB0SNsr6q_93ppzo6jdQ=="

token = INFLUXDB_TOKEN
org = "test"
bucket = "timeseriesData"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

app = Flask(__name__)

@app.route("/fetch_data", methods=["GET"])
def fetch_data():
    # Get start and stop times from query parameters
    start = request.args.get("start")
    stop = request.args.get("stop")

    # If either start or stop is missing, return an error
    if not start or not stop:
        return jsonify({"error": "Please provide both start and stop query parameters"}), 400

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
                results.append((record.get_field(), record.get_value()))

        # Print the results for debugging
        print("Fetched data:", results)

        return jsonify(results)
    except Exception as e:
        print("Error fetching data:", str(e))
        return jsonify({"error": "Error fetching data"}), 500

class FlaskThread(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.srv = make_server('127.0.0.1', 5000, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        print("Starting server")
        self.srv.serve_forever()

    def shutdown(self):
        print("Shutting down server")
        self.srv.shutdown()

#flask_thread = FlaskThread(app)
#flask_thread.start()
