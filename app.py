import time
from http import HTTPStatus

from flask import Flask, request, json
from flask import Response

from coordinator import Coordinator

app = Flask(__name__)
coordinator = Coordinator()
cache = {}


@app.route('/')
def index():
    resp = Response()
    resp.set_data('Key Value Store')
    return resp


@app.route('/put', methods=['PUT'])
def put():
    r = Response(mimetype="application/json")
    r.headers["Content-Type"] = "text/json; charset=utf-8"

    data = request.values

    if 'key' not in data or 'val' not in data:
        r.status_code = HTTPStatus.BAD_REQUEST
        r.response = json.dumps({'code': "1", 'message': "key or val is empty"})
        return r
    key = data['key']
    val = data['val']

    return coordinator.put(key, val)


@app.route('/cache/print', methods=['GET'])
def cachePrint():
    r = Response(mimetype="application/json")
    r.headers["Content-Type"] = "text/json; charset=utf-8"

    r.response = json.dumps(cache)
    return r


@app.route('/cache/put', methods=['PUT'])
def cachePut():
    r = Response(mimetype="application/json")
    r.headers["Content-Type"] = "text/json; charset=utf-8"

    data = request.values
    if data is None:
        r.status_code = HTTPStatus.BAD_REQUEST
        r.response = json.dumps({'code': "1", 'message': "data not provided"})
        return r

    # data = json.loads(data)
    if 'key' not in data or 'val' not in data:  # eventual consistency
        r.status_code = HTTPStatus.BAD_REQUEST
        r.response = json.dumps({'code': "1", 'message': "key or val is empty"})
        return r

    key = data['key']
    val = data['val']
    cache[key] = {'val': val, 'tick': time.time_ns()}
    r.status_code = HTTPStatus.OK

    return r


@app.route('/get', methods=['GET'])
def get():
    data = request.values

    if 'key' not in data:
        r = Response(mimetype="application/json")
        r.headers["Content-Type"] = "text/json; charset=utf-8"
        r.response = json.dumps({'code': "1", 'message': "key is empty"})
        return r
    key = data['key']

    return coordinator.get(key)


@app.route('/cache/get', methods=['GET'])
def cacheGet():
    r = Response(mimetype="application/json")
    r.headers["Content-Type"] = "text/json; charset=utf-8"

    data = request.values

    if 'key' not in data:
        r.response = json.dumps({'code': "1", 'message': "key is empty"})
        return r
    key = data['key']

    if key not in cache:
        r.response = json.dumps({'code': "1", 'message': "key not in cache"})
        return r
    else:
        r.response = json.dumps({'code': "0", 'val': str(cache[key])})
    return r


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
