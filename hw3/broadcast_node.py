import grpc
from proto import broadcast_pb2, broadcast_pb2_grpc
from google.protobuf import empty_pb2

from concurrent import futures
from dataclasses import dataclass
from typing import List, Dict, Set
from collections import deque

@dataclass
class MessageUid:
    originId: int
    originSqnNum: int

class BroadcastNode():
    def __init__(self, id: int, all_ids: List[int]):
        self._id = id
        self._all_ids = sorted(all_ids)

        self._next_sequence_number = 1
        self._vector_clock= [0 for _ in range(len(all_ids))]

        self._replicated_hosts: Dict[MessageUid, Set[int]] = {}
        self._not_commited_messages: Dict[MessageUid, broadcast_pb2.BroadcastMessage] = {}
        self._commited_messages: Set[broadcast_pb2.BroadcastMessage] = set()
        self._delivered_messages: deque[broadcast_pb2.BroadcastMessage] = deque()

        self._send_queues: Dict[int, deque] = {deque() for id in self._all_ids if id != self._id}

    def submit_client_message(self, msg: broadcast_pb2.ClientMessage):
        broadcast_msg = broadcast_pb2.BroadcastMessage()
        broadcast_msg.message = msg
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

    def handle_broadcast_message(self, broadcast_msg: broadcast_pb2.BroadcastMessage):
        # TODO: detect delivered messages (broadcast may have been received from stale node) cleanup self._replicated_hosts
        msg_uid = MessageUid(broadcast_msg.metadata.originId, broadcast_msg.metadata.originSqnNum)
        is_first_time = not (msg_uid in self._replicated_hosts)

        old_replicated_hosts = self._replicated_hosts[msg_uid] if msg_uid in self._replicated_hosts else set()
        new_replicated_hosts = set(broadcast_msg.metadata.knownReplicatedOnHosts)
        new_replicated_hosts.add(self._id)
        new_replicated_hosts = new_replicated_hosts | old_replicated_hosts
        self._replicated_hosts[msg_uid] = new_replicated_hosts

        if is_first_time:
            self._not_commited_messages[msg_uid] = broadcast_msg
            ack_msg = broadcast_pb2.BroadcastMessage()
            ack_msg.CopyFrom(broadcast_msg)
            ack_msg.message.Clear()
            ack_msg.metadata.knownReplicatedOnHosts = self._replicated_hosts[msg_uid]
            broadcast_msg.metadata.knownReplicatedOnHosts = self._replicated_hosts[msg_uid]
            for id in self._all_ids:
                if id != self._id:
                    # Optimization: don't send message payload to node we know replicated it
                    if id in self._replicated_hosts[msg_uid]:
                        self._send_queues[id].append(ack_msg)
                    else:
                        self._send_queues[id].append(broadcast_msg)

        if not len(old_replicated_hosts) * 2 > len(self._all_ids) and len(new_replicated_hosts) * 2 > len(self._all_ids):
            self.commit(self._not_commited_messages[msg_uid])
            self._not_commited_messages.pop(msg_uid)


    def commit(self, msg: broadcast_pb2.BroadcastMessage):
        self._commited_messages.add(msg)
        changed = True
        while changed:
            changed = False
            for msg in self._commited_messages:
                if all([msg.metadata.vectorClock[i] <= self._vector_clock[i] for i in range(len(self._all_ids))]):
                    self.deliver(msg)
                    self._commited_messages.remove(msg)
                    changed = True
                    break

    def deliver(self, msg: broadcast_pb2.BroadcastMessage):
        self._delivered_messages.append(msg.message)
        self._vector_clock[self._all_ids.index(msg.metadata.originId)] += 1

    # TODO: work on _send_queue
    # TODO: consume delivery queue method
    # TODO: grpc service + client
