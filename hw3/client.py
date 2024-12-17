from broadcast_node import BroadcastNode
from proto.broadcast_pb2 import ClientMessage

from flask import Flask, request, jsonify

import argparse
import json
import threading

app = Flask(__name__)

id = 0
node: BroadcastNode = None

mutex = threading.Lock()
state_origin_id: dict = {}
state: dict = {}

def consume_delivered():
    while True:
        msg = node.try_get_delivered()
        if msg:
            mutex.acquire()
            for key, val in zip(msg.keys, msg.vals):
                if state_origin_id.get(key, 0) <= msg.originId:
                    state[key] = val
                    state_origin_id[key] = msg.originId
            mutex.release()
        else:
            return

@app.route('/', methods=["GET"])
def get():
    mutex.acquire_lock()
    response = state
    mutex.release()
    return response

@app.route('/', methods=["PATCH"])
def patch():
    data = request.get_json()
    assert(type(data) == dict)
    msg = ClientMessage(originId=id)
    for key, val in data.items():
        msg.keys.append(key)
        msg.vals.append(val)
    replicated = node.submit_message(msg)
    mutex.acquire_lock()
    response = state
    mutex.release()
    return {
        "replicated": replicated,
        "state": response
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--total-ids", type=int, default=3)
    args = parser.parse_args()
    assert(args.id > 0 and args.id <= args.total_ids)
    id = args.id
    node = BroadcastNode(id=id, all_ids=[i for i in range(1, args.total_ids + 1)])

    def keep_consuming_delivered():
        while True:
            consume_delivered()

    threading.Thread(target=keep_consuming_delivered).start()
    app.run(port=8000 + args.id)
