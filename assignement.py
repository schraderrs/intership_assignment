import psycopg2
import requests
import json
import csv
import datetime


filepath = 'ratings_revised.json'
json_data = '{"a": 1, "b": 2, "c": 3, "d": 4}'

# Connection to the database
try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="test",
        port="5432"
    )
    # connection.autocomit = True

    print('Connected')

    cursor = conn.cursor()
    print(conn.get_dsn_parameters())

    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print('Connected to -', record)

except (Exception, psycopg2.Error) as error:
    print("Not connected to PostgreSQL", error)

# Close connection
finally:
    if(conn):
        cursor.close()
        conn.close()
        print("Connection closed")

# Converter from csv to json 
# ratings.json is actually a csv 
csvFilePath = 'ratings.json'
jsonFilePath = 'ratings_revised.json'

data = {}
with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for csvRow in csvReader:
        movie_id = csvRow['movie_id']
        data[movie_id] = csvRow
        data[movie_id]['timestamp'] = datetime.datetime.fromtimestamp(float(csvRow['timestamp'])).strftime("%Y-%m-%d %H:%M:%S")
root = {}
root["ratings"] = data

with open(jsonFilePath, "w") as jsonFile:
    jsonFile.write(json.dumps(root, indent = 4))

# Import json file into database
data = []
with open('ratings_revised.json') as f:
    for line in f:
        data.append(json.loads(line))

fields = [
    "rating"
    "movie_id"
    "timestamp"
    "user_id"
]

for item in data:
    my_data = [item[field] for field in fields]
    for i, v in enumerate(my_data):
        if isinstance(v, dict):
            my_data[i] = json.dumps(v)
    insert_query = "INSERT INTO crm VALUES (%s, %s, %s, %s)"
    cursor.execute(insert_query, tuple(my_data))
