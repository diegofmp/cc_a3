import flask
from flask import request, jsonify, make_response, abort
from datetime import datetime
import requests
import os
import csv
from custom_logger import Custom_Logger, Log_record


app = flask.Flask(__name__)
app.config["DEBUG"] = True

# setup
database_url = os.getenv("DATABASE-SVC", "http://database")

# internal functions ###########################################################
num_buckets = 4
'''
input: key
output: target database bucket id

Using the modulo operator it calculates the target database's bucket
'''
def hash_function(key):
    return key % num_buckets


def getTime():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def log(something):
    app.logger.info(something)

# initialize custom logger instance
logger = Custom_Logger("logs.txt")


####################### Endpoints ##############################################
@app.route("/test",  methods = ['GET'])
def test_endpoint():
    return "Grettings from API service!"


@app.route("/insert",  methods = ['POST'])
#@expects_json(frame_schema)
#def insert():
def insert_handler():
    # take the key and value a from request
    req = request.get_json()

    key = req["k"]
    a = req["a"]

    #return response
    return insert(key=key, value=a)
    
def database_summary():
    # get folders summary
    response =  requests.get(database_url + "/content_summary")

    # log it!
    json_response = response.json()
    for level in json_response:
        log(level)
        if (len(level[1])==0): # if we are in a bucket, not root:
            message = "Bucket: "+ str(level[0]) + " - files: "+ str(level[2])
            logger.write(message)


def insert(key, value):
    # Process: get target key
    target_bucket = hash_function(key)

    # Send to database bucket
    # prepare request 
    db_request = {
        "key" : key,
        "value": value,
        "target": target_bucket
    }

    log("DB_REQUEST: ")
    log(db_request)
    
    # send request
    response = requests.post(database_url + "/insert", json=db_request)
    json_response = response.json()
    log("JSON_RESPONSE OF DB: ")
    log(json_response)
    if(response.ok):
        log_content = "Insert: key:[" + str(key) + "] - filename:[" + str(key)+ "]  - node:[" + str(target_bucket) + "] - [success]"
        logger.write(log_content)

    response = {
        "timestamp": getTime(),
    }
    
    return response

@app.route("/range",  methods = ['GET'])
def range_search():
    k1 = request.args.get('k1') # from
    k2 = request.args.get('k2') # to

    
    k1 = int(k1)
    k2 = int(k2)

    # validate k1 and k2
    if(k2 < k1 or k1< 0 or k2<0):
        message = "Invalid params!. Consider that: K1 and K2 shold be positive numbers. K2 must be greater or equal than K1"
        abort(make_response(jsonify(message=message), 400))

    log("[API] range k: ")
    log(k1)
    log(k2)

    k_s = [k for k in range(k1, k2+1)] # list all keys in range

    buckets = [hash_function(k) for k in k_s] # calculate buckets for each

    # prepare request 
    db_request = {
        "keys" : k_s,
        "buckets": buckets
    }

    log("DB_REQUEST: ")
    log(db_request)
    
    # send request
    response = requests.post(database_url + "/range",  json=db_request)
    json_response = response.json()
    log("JSON_RESPONSE OF DB: ")
    log(json_response)

    if(json_response.get("contents")):
        return json_response
    else:
        error_type = json_response.get("error_type")
        message = json_response.get("message")
        abort(make_response(jsonify(message=message, error_type=error_type), 400))

@app.route("/search/<k>",  methods = ['GET'])
def search(k):
    log("[API] searching k: ")
    log(k)
    k = int(k)
    log("[API] searching k: ")
    log(k)

    bucket = hash_function(k)

    # prepare request 
    db_request = {
        "key" : k,
        "bucket": bucket
    }

    log("DB_REQUEST: ")
    log(db_request)
    
    # send request
    response = requests.get(database_url + "/search", params=db_request)
    json_response = response.json()
    log("JSON_RESPONSE OF DB: ")
    log(json_response)

    if(json_response.get("content")):
        # log access
        log_content = "Read_: key:[" + str(k) + "] - value: "+ json_response.get("content") + " - filename:[" + str(k)+ "]  - node:[" + str(bucket) + "] - [success]"
        logger.write(log_content)

        return json_response
    else:
        error_type = json_response.get("error_type")
        message = json_response.get("message")
        abort(make_response(jsonify(message=message, error_type=error_type), 400))

@app.route("/delete/<k>",  methods = ['DELETE'])
def delete(k):
    k = int(k)
    bucket = hash_function(k)

    # prepare request 
    db_request = {
        "key" : k,
        "bucket": bucket
    }

    # send request
    response = requests.delete(database_url + "/delete", params=db_request)
    json_response = response.json()
    log("JSON_RESPONSE OF DB: ")
    log(json_response)

    if response.ok:
        # log it!
        log_content = "Delete: key:[" + str(k) + "] - value: "+ json_response.get("content") + " - filename:[" + str(k)+ "]  - node:[" + str(bucket) + "] - [success]"
        logger.write(log_content)

        return json_response
    else:
        error_type = json_response.get("error_type")
        message = json_response.get("message")
        abort(make_response(jsonify(message=message, error_type=error_type), 400))

### endpoint to laod data (populate storage) from file
@app.route("/populate",  methods = ['GET'])
def populate():
    # log start of process:
    logger.write("Start populating data...|")

    #PARAMS:
    batch_size = 50 # size of splits that would be stored w unique key.

    # get file from external source
    file_name = "demo.csv"

    # process csv splitting it into batches. Then store them on the distributed storage.
    try:
        read_csv_in_batches(file_name, batch_size)

        # after reading. Get status of buckets!
        database_summary()

    except Exception as e:
        error_message = e
        abort(make_response(jsonify(message=error_message, error_type="unknown"), 400))
    
    return "Storage population has started!!"

def read_csv_in_batches(filename, batch_size):
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        #header = next(reader)
        batch = []
        batch_index = 0
        for i, row in enumerate(reader):
            batch.append(row)
            if (i + 1) % batch_size == 0:
                try:
                    insert(key=batch_index, value=batch)
                except Exception as e:
                    # log error and continue loop.
                    log("Error inserting %d" % batch_index)
                    log(e)

                batch = []
                batch_index += 1
        # handle the last batch if its size is less than batch_size
        if batch:
            insert(key=batch_index, value=batch)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)