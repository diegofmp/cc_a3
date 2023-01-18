import flask
from flask import request, jsonify, make_response, abort
from datetime import datetime

app = flask.Flask(__name__)
app.config["DEBUG"] = True

####################### Endpoints ##############################################
@app.route("/test",  methods = ['GET'])
def test_endpoint():
    return "Grettings from DATABASE service!"

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80)