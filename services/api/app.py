import flask
from flask import request, jsonify, make_response, abort
from datetime import datetime
import requests
import os


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


####################### Endpoints ##############################################
@app.route("/test",  methods = ['GET'])
def test_endpoint():
    return "Grettings from API service!"


@app.route("/insert",  methods = ['POST'])
#@expects_json(frame_schema)
def insert():
    # take the key and value a from request
    req = request.get_json()

    key = req["k"]
    a = req["a"]


    # Process: get target key
    target_bucket = hash_function(key)

    # Send to database bucket
    # prepare request 
    db_request = {
        "key" : key,
        "value": a,
        "target": target_bucket                
    }

    log("DB_REQUEST: ")
    log(db_request)
    
    # send request
    response = requests.post(database_url + "/insert", json=db_request)
    json_response = response.json()
    log("JSON_RESPONSE OF DB: ")
    log(json_response)

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
        return json_response
    else:
        error_type = json_response.get("error_type")
        message = json_response.get("message")
        abort(make_response(jsonify(message=message, error_type=error_type), 400))


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)