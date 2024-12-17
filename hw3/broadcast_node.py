import grpc
from proto import broadcast_pb2, broadcast_pb2_grpc
from google.protobuf import empty_pb2

from concurrent import futures
from dataclasses import dataclass
from typing import List, Dict, Set
from collections import deque
from threading import Lock, Thread

import datetime

@dataclass(eq=True, frozen=True)
class MessageUid:
    originId: int
    originSqnNum: int


class _Node(broadcast_pb2_grpc.NodeServicer):
    def __init__(self, node):
        self._node = node

    def Broadcast(self, request, context):
        # print("Broadcast received")
        self._node.handle_broadcast_message(request)
        return empty_pb2.Empty()

class BroadcastNode():

    def __init__(self, id: int, all_ids: List[int]):
        self._id = id
        self._all_ids = sorted(all_ids)

        self._next_sequence_number = 1
        self._vector_clock= [0 for _ in range(len(all_ids))]

        self._replicated_hosts: Dict[MessageUid, Set[int]] = {}
        self._not_commited_messages: Dict[MessageUid, broadcast_pb2.BroadcastMessage] = {}
        self._commited_messages: Dict[MessageUid, broadcast_pb2.BroadcastMessage] = {}
        self._delivered_messages: deque[broadcast_pb2.BroadcastMessage] = deque()

        print("creating grpc stubs")
        self._stubs = {id: broadcast_pb2_grpc.NodeStub(grpc.insecure_channel("localhost:" + str(50000 + id))) for id in self._all_ids if id != self._id}
        self._send_queues: Dict[int, deque] = {id: deque() for id in self._all_ids if id != self._id}
        self._send_last_unsuccessfull_time = {id: datetime.datetime(1,1,1) for id in self._all_ids if id != self._id}
        self._mutex = Lock()

        port = str(50000 + id)
        self._grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        broadcast_pb2_grpc.add_NodeServicer_to_server(_Node(self), self._grpc_server)
        self._grpc_server.add_insecure_port("[::]:" + port)
        print("starting grpc server")
        self._grpc_server.start()

        self._workThread = Thread(target=self.work)
        print("starting work thread")
        self._workThread.start()


    def submit_message(self, msg: broadcast_pb2.ClientMessage):
        self._mutex.acquire()
        print("client submitted a message")
        broadcast_msg = broadcast_pb2.BroadcastMessage()
        broadcast_msg.message.CopyFrom(msg)
        broadcast_msg.metadata.originId = self._id
        broadcast_msg.metadata.originSqnNum = self._next_sequence_number; self._next_sequence_number += 1
        broadcast_msg.metadata.knownReplicatedOnHosts.append(self._id)
        # all messages delivered before current happen before it
        vector_clock = self._vector_clock.copy()
        # all messages on the same node happen before it
        vector_clock[self._all_ids.index(self._id)] = broadcast_msg.metadata.originSqnNum - 1
        broadcast_msg.metadata.vectorClock.extend(self._vector_clock)

        for id in self._all_ids:
            if id != self._id:
                self._send_queues[id].append(broadcast_msg)
        msg_uid = MessageUid(broadcast_msg.metadata.originId, broadcast_msg.metadata.originSqnNum)
        self._replicated_hosts[msg_uid] = set([self._id])
        self._not_commited_messages[msg_uid] = broadcast_msg
        self._mutex.release()

    def try_get_delivered(self):
        self._mutex.acquire()
        acquired = None
        if len(self._delivered_messages) > 0:
            print("client consumed a delivered message")
            acquired = self._delivered_messages.popleft()
        self._mutex.release()
        return acquired

    def handle_broadcast_message(self, broadcast_msg: broadcast_pb2.BroadcastMessage):
        # TODO: detect delivered messages (broadcast may have been received from stale node) cleanup self._replicated_hosts
        self._mutex.acquire()
        print("incoming broadcast message")
        msg_uid = MessageUid(broadcast_msg.metadata.originId, broadcast_msg.metadata.originSqnNum)
        is_first_time = not (msg_uid in self._replicated_hosts)

        old_replicated_hosts = self._replicated_hosts[msg_uid] if msg_uid in self._replicated_hosts else set()
        new_replicated_hosts = set(broadcast_msg.metadata.knownReplicatedOnHosts)
        new_replicated_hosts.add(self._id)
        new_replicated_hosts = new_replicated_hosts | old_replicated_hosts
        self._replicated_hosts[msg_uid] = new_replicated_hosts

        if is_first_time:
            self._not_commited_messages[msg_uid] = broadcast_msg
            broadcast_msg.metadata.knownReplicatedOnHosts.extend([
                obj for obj in self._replicated_hosts[msg_uid] if obj not in broadcast_msg.metadata.knownReplicatedOnHosts
            ])
            ack_msg = broadcast_pb2.BroadcastMessage()
            ack_msg.CopyFrom(broadcast_msg)
            ack_msg.message.Clear()
            for id in self._all_ids:
                if id != self._id:
                    # Optimization: don't send message payload to node we know replicated it
                    if id in self._replicated_hosts[msg_uid]:
                        self._send_queues[id].append(ack_msg)
                    else:
                        self._send_queues[id].append(broadcast_msg)

        if not len(old_replicated_hosts) * 2 > len(self._all_ids) and len(new_replicated_hosts) * 2 > len(self._all_ids):
            self.commit(msg_uid)
            self._not_commited_messages.pop(msg_uid)
        self._mutex.release()


    def commit(self, msg_uid: MessageUid):
        print("message commited")
        self._commited_messages[msg_uid] = self._not_commited_messages[msg_uid]
        changed = True
        while changed:
            changed = False
            for msg_uid, msg in self._commited_messages.items():
                if all([msg.metadata.vectorClock[i] <= self._vector_clock[i] for i in range(len(self._all_ids))]):
                    self.deliver(msg)
                    self._commited_messages.pop(msg_uid)
                    changed = True
                    break


    def deliver(self, msg: broadcast_pb2.BroadcastMessage):
        print("message delivered")
        self._delivered_messages.append(msg)
        self._vector_clock[self._all_ids.index(msg.metadata.originId)] += 1


    def work(self):
        while True:
            self._mutex.acquire()
            for id, queue in self._send_queues.items():
                if len(queue) > 0 and (datetime.datetime.now() - self._send_last_unsuccessfull_time[id]) > datetime.timedelta(seconds=5):
                    print("sending message to node", id)
                    msg = queue[0]
                    self._mutex.release()
                    failed = False
                    try:
                        self._stubs[id].Broadcast(msg)
                    except:
                        failed = True
                    self._mutex.acquire()
                    if failed:
                        self._send_last_unsuccessfull_time[id] = datetime.datetime.now()
                    else:
                        self._send_queues[id].remove(msg)
            self._mutex.release()

