from flask import Flask, jsonify, request
from influxdb_client import InfluxDBClient
from werkzeug.serving import make_server

INFLUXDB_TOKEN = "ng00FvBO_y9QMljRt8b524Kg2fAfubNaz4XM171H8qmF6o6Dr9L_4cYPuIt4NPAtRlOB0SNsr6q_93ppzo6jdQ=="

token = INFLUXDB_TOKEN
org = "test"
bucket = "timeseriesData"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

app = Flask(__name__)

@app.route("/fetch_data")
def fetch_data():
    start_time = request.args.get("startTime")
    end_time = request.args.get("endTime")

    query = f'''
    from(bucket: "{bucket}")
    |> range(start: "{start_time}", stop: "{end_time}")
    |> filter(fn: (r) => r["_measurement"] == "energy_data")
    |> filter(fn: (r) => r["_field"] == "RH_2" or r["_field"] == "RH_3")
    |> filter(fn: (r) => r["deviceId"] == "1155")
    |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
    |> yield(name: "mean")
    '''

    print("InfluxDB Query:", query)

    # Execute the query
    result = query_api.query(query)

    # Format the result
    response = []
    for table in result:
        for record in table.records:
            response.append({
                "time": record.get_time(),
                "field": record.get_field(),
                "value": record.get_value()
            })

    # Return the result as a JSON response
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)
