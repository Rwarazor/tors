from broadcast_node import BroadcastNode, CausalityInfo
from proto.broadcast_pb2 import ClientMessage

from flask import Flask, request, jsonify

import argparse
import json
import threading
import time

app = Flask(__name__)

id = 0
node: BroadcastNode = None

mutex = threading.Lock()
state_causality: dict = {}
state: dict = {}


def strictly_later(causality_left: CausalityInfo, causality_right: CausalityInfo):
    return all(
        [
            causality_left.vector_clock[i] >= causality_right.vector_clock[i]
            for i in range(len(causality_left.vector_clock))
        ]
    ) and any(
        [
            causality_left.vector_clock[i] > causality_right.vector_clock[i]
            for i in range(len(causality_left.vector_clock))
        ]
    )

def later(causality_left: CausalityInfo, causality_right: CausalityInfo):
    if strictly_later(causality_left, causality_right):
        return True
    if strictly_later(causality_right, causality_left):
        return False
    return causality_left.originId > causality_right.originId


def consume_delivered():
    while True:
        msg, causality = node.try_get_delivered()
        if msg:
            print("causality_received", causality)
            mutex.acquire()
            for key, val in zip(msg.keys, msg.vals):
                if not key in state_causality or later(causality, state_causality[key]):
                    state[key] = val
                    state_causality[key] = causality
            mutex.release()
        else:
            return


@app.route("/", methods=["GET"])
def get():
    mutex.acquire_lock()
    response = state
    mutex.release()
    return response


@app.route("/", methods=["PATCH"])
def patch():
    data = request.get_json()
    assert type(data) == dict
    msg = ClientMessage()
    for key, val in data.items():
        msg.keys.append(key)
        msg.vals.append(val)
    replicated = node.submit_message(msg)
    consume_delivered()
    mutex.acquire_lock()
    response = state
    mutex.release()
    return {"replicated": replicated, "state": response}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--total-ids", type=int, default=3)
    args = parser.parse_args()
    assert args.id > 0 and args.id <= args.total_ids
    id = args.id
    node = BroadcastNode(id=id, all_ids=[i for i in range(1, args.total_ids + 1)])

    def keep_consuming_delivered():
        while True:
            consume_delivered()
        time.sleep(0.1)

    threading.Thread(target=keep_consuming_delivered).start()
    app.run(port=8000 + args.id)
