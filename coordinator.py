import os
import queue
import random
import threading
import time
import traceback
from http import HTTPStatus
from multiprocessing import Pool

import requests
from flask import Response, json


class Coordinator:
    # N=number of replicas
    # W=write quorum size
    # R=read quorum size
    # W+R>N

    def send_put_request(self, url, params, queue):
        print("Thread", threading.current_thread())  # TODO: check for eventual consistency
        if url == 'http://localhost:8080':
            time.sleep(random.random() * 10)

        x = requests.post(url + "/cache/put", data=params, timeout=1)
        if x.status_code == 200:
            queue.put((id, 'done'))
        else:
            queue.put((id, 'not done'))
            print(url + " not done" + x.status_code)
        return

    def send_get_request(self, url, params, queue):
        print("Thread", threading.current_thread())  # TODO: check for eventual consistency
        if url == 'http://localhost:8080':
            time.sleep(random.random() * 10)

        x = requests.get(url + "/cache/get", data=params)
        if x.status_code == 200:
            print(url + " done")
            queue.put((id, x.content))
        else:
            print(url + " not done " + str(x.status_code))
        return

    def __init__(self):
        self.serverUrls = os.environ.get('SERVER_URLS') if os.environ.get(
            'SERVER_URLS') is not None else "http://localhost:8080,http://localhost:8081,http://localhost:8082"
        print('SERVER_URLS:' + self.serverUrls)

        self.N = os.environ.get('N') if os.environ.get('N') is not None else 3
        self.W = os.environ.get('W') if os.environ.get('W') is not None else 2
        self.R = os.environ.get('R') if os.environ.get('R') is not None else 2

    def put(self, key, value):
        r = Response(mimetype="application/json")
        r.headers["Content-Type"] = "text/json; charset=utf-8"

        print("putting into co-ordinator")
        q = queue.Queue(maxsize=len(self.serverUrls.split(',')))
        with Pool(processes=2) as pool:
            threads = [threading.Thread(target=self.send_put_request, args=(url, {"key": key, "val": value}, q)) for
                       url in
                       self.serverUrls.split(',')]
            for th in threads:
                # th.daemon = True
                th.start()

            print("threads are created ")
            # count = self.W
            success = 0
            failure = 0
            while success != self.W and success + failure != self.W:
                try:
                    x = q.get(True, 4)
                    success = success + 1
                except Exception as e:
                    failure = failure + 1
                    print(traceback.format_exc())

        if success != self.W:
            print("could not put into servers")
            r.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            r.response = json.dumps({'code': "1", 'message': "write quorum fail"})
        else:
            print("putting has succeeded")
            r.status_code = HTTPStatus.OK
            r.response = json.dumps({'code': "0", 'message': "success"})
        return r

    def resolveCacheEntries(self, results):
        if all(json.loads(elem)['val'] == json.loads(results[0])['val'] for elem in results):
            return True
        else:
            return False

    def get(self, key):
        r = Response(mimetype="application/json")
        r.headers["Content-Type"] = "text/json; charset=utf-8"

        with Pool(processes=2) as pool:
            q = queue.Queue(maxsize=len(self.serverUrls.split(',')))
            threads = [threading.Thread(target=self.send_get_request, args=(url, {"key": key}, q)) for url in
                       self.serverUrls.split(',')]
            for th in threads:
                th.daemon = True
                th.start()

            outputs = []

            count = self.R
            while count > 0:
                retry = 3
                while True:
                    if retry <= 0:
                        r.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
                        r.response = json.dumps({'code': "1", 'message': "read quorum fail"})
                        return r
                    x = q.get(False)
                    if x is None:
                        print("retrying get in the co-ordinator...")
                        retry = retry - 1
                    else:
                        print('hmmm')
                        outputs.append(x)
                        count = count - 1
                        break
            print(outputs)
        return r
