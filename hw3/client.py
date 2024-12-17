from broadcast_node import BroadcastNode
from proto.broadcast_pb2 import ClientMessage

from flask import Flask, request, jsonify

import argparse
import json
import threading

app = Flask(__name__)

mutex = threading.Lock()
node: BroadcastNode = None

state: dict = {}

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
    assert(len(data) == 1)
    for key, val in data.items():
        msg = ClientMessage(key=key,val=val)
        node.submit_message(msg)
        # TODO: wait for commit with timeout
        # TODO: return patched state
        return {}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--total-ids", type=int, default=5)
    args = parser.parse_args()
    assert(args.id > 0 and args.id <= args.total_ids)

    node = BroadcastNode(id=args.id, all_ids=[i for i in range(1, args.total_ids + 1)])

    def consume_delivery():
        while True:
            val = node.try_get_delivered()
            if val:
                mutex.acquire()
                # TODO: conflict resolution
                state[val.message.key] = val.message.val
                mutex.release()

    threading.Thread(target=consume_delivery).start()
    app.run(port=8000 + args.id)
