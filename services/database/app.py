import flask
from flask import request, jsonify, make_response, abort
from datetime import datetime
import os
import csv

app = flask.Flask(__name__)
app.config["DEBUG"] = True

## General params
num_buckets = 4
root_buckets_path = os.path.join(os.getcwd(), 'buckets')


### UTILS
def getTime():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def log(something):
    app.logger.info(something)

## initial check of the main folder
def check_root_directory():
    log("--------- check root directory....")
    target_dir = os.path.join(os.getcwd(), 'buckets')
    log("TARGET_DIR: ")
    log(target_dir)

    if not os.path.exists(target_dir):
        
        try:
            log("folder doesmnt exsit!!!!. gonna create")

            # create root folder and subfolders (buckets)
            for i in range(num_buckets):
                target_subdir = os.path.join(target_dir, str(i))
                log(target_subdir)
                os.makedirs(target_subdir)
        except Exception as e:
            log("Error creating folder")
            log(e)
    else:
        log("Root folder already exists!!")
        

# list content of directory
def list_directory_content(path):
    content = [[root, dirs, files] for root, dirs, files in os.walk(path)]
    return content

@app.route("/content_summary",  methods = ['GET'])
def content_summary():
    target_dir = os.path.join(os.getcwd(), 'buckets')
    #list_directory_content(target_dir)
    return list_directory_content(target_dir)
        

####################### Endpoints ##############################################
@app.route("/test",  methods = ['GET'])
def test_endpoint():
    return "Grettings from DATABASE service!"

@app.route("/search",  methods = ['GET'])
def search():
    key = request.args.get('key')
    bucket = request.args.get('bucket')

    try:
        log("Trying to read....")
        filename = os.path.join(root_buckets_path, str(bucket), str(key))
        log(filename)
        file = open(filename, mode='r')
        content = file.read()
        log(content)
        file.close()

        response = {
            "content": content,
            "timestamp": getTime(),
        }
        
        return response

    except FileNotFoundError as e:
        error_message = "Key not found!"
        log("ERROR: file not found!")
        log(e)
        abort(make_response(jsonify(message=error_message, error_type="not_found"), 400))
        
    except Exception as e: # General error
        error_message = "An exception occurred"
        log("ERROR reading")
        log(e)
        abort(make_response(jsonify(message=error_message, error_type="internal_error"), 400))




@app.route("/insert",  methods = ['POST'])
#@expects_json(frame_schema)
def insert():
    try:
        log(">>>>>>>>>>>> init insert....")
        # take the key and value a from request
        req = request.get_json()

        key = req["key"] # will be the filename
        value = req["value"] # content
        target = req["target"] # bucket id

        log(".....>> gonna try to store!")

        # insert file into corresponding target folder
        # TODO

        file_type = "csv"

        if file_type=="csv":
            with open(os.path.join(root_buckets_path, str(target), str(key)), 'w', newline='') as fp:
                writer = csv.writer(fp)
                for line in value:
                    writer.writerow(line)

        else:
            with open(os.path.join(root_buckets_path, str(target), str(key)), 'w') as fp:
                fp.write(value)
            
        log("STORING....")
        log("request: ")
        log(req)

        response = {
            "timestamp": getTime(),
        }
        
        return response

    except Exception as e:
        error_message = "An exception occurred"
        log("ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
        log(e)
        abort(make_response(jsonify(message=error_message), 400))

    response = {
        "timestamp": getTime(),
    }
    
    return response


@app.route("/range",  methods = ['POST'])
def range_search():
    req = request.get_json()

    keys = req["keys"]
    buckets = req["buckets"]
    log("Keys: ")
    log(keys)
    log("Buvkets: ")
    log(buckets)

    contents = []
    for item in list(zip(keys, buckets)):
        log(item)
        key = int(item[0])
        bucket = int(item[1])

        try:
            log("Trying to read....")
            filename = os.path.join(root_buckets_path, str(bucket), str(key))
            log(filename)
            file = open(filename, mode='r')
            content = file.read()
            log(content)
            file.close()

            content_item = {
                "key": key,
                "content": content
            }
            contents.append(content_item)

        except FileNotFoundError as e:
            error_message = "Key not found!"
            log("ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR: file not found!")
            log(e)
            pass
            #abort(make_response(jsonify(message=error_message, error_type="not_found"), 400))
            
        except Exception as e: # General error
            error_message = "An exception occurred"
            log("Internal error!!! stop")
            log(e)
            abort(make_response(jsonify(message=error_message, error_type="internal_error"), 400))

    response = {
        "contents": contents,
        "timestamp": getTime(),
    }

    return response


@app.route("/delete",  methods = ['DELETE'])
def delete():
    key = request.args.get('key')
    bucket = request.args.get('bucket')
    content = None

    try:
        log("Trying to delete....")
        filename = os.path.join(root_buckets_path, str(bucket), str(key))
        log(filename)
        if os.path.isfile(filename):
            file = open(filename, mode='r')
            content = file.read()
            file.close()

            os.remove(filename)
        else:
            error_message = "Key not found!"
            abort(make_response(jsonify(message=error_message), 400))
        
        response = {
            "removed": key,
            "content": content,
            "timestamp": getTime(),
        }
        
        return response

    except Exception as e: # General error
        error_message = "An exception occurred"
        log("ERRORRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR deleting")
        log(e)
        abort(make_response(jsonify(message=error_message, error_type="internal_error"), 400))


if __name__ == '__main__':
    check_root_directory()
    app.run(host="0.0.0.0", port=80)