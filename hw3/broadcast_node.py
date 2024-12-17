import grpc
from proto import broadcast_pb2, broadcast_pb2_grpc
from google.protobuf import empty_pb2

from concurrent import futures
from dataclasses import dataclass, field
from typing import List, Dict, Set
from collections import deque
from threading import Lock, Thread

import datetime
import time
import random


class _Node(broadcast_pb2_grpc.NodeServicer):
    def __init__(self, node):
        self._node = node

    def Broadcast(self, request, context):
        # print("Broadcast received")
        self._node.handle_broadcast_message(request)
        time.sleep(random.randint(100, 1000) / 1000)
        return empty_pb2.Empty()


@dataclass(eq=True, frozen=True)
class MessageUid:
    originId: int
    originSqnNum: int


@dataclass
class MessageState:
    message: broadcast_pb2.BroadcastMessage | None = None
    known_replicated_hosts: Set[int] = field(default_factory=set)
    acks_received: int = 1
    is_delivered: bool = False


def _get_msg_uid(msg: broadcast_pb2.BroadcastMessage):
    return MessageUid(msg.metadata.originId, msg.metadata.originSqnNum)


class BroadcastNode:
    def _init_grpc(self):
        print("creating grpc server")
        port = str(50000 + self._id)
        self._grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        broadcast_pb2_grpc.add_NodeServicer_to_server(_Node(self), self._grpc_server)
        self._grpc_server.add_insecure_port("[::]:" + port)

        print("creating grpc stubs")
        self._stubs = {
            id: broadcast_pb2_grpc.NodeStub(
                grpc.insecure_channel("localhost:" + str(50000 + id))
            )
            for id in self._all_ids
            if id != self._id
        }
        self._send_queues: Dict[int, deque[broadcast_pb2.BroadcastMessage]] = {
            id: deque() for id in self._all_ids if id != self._id
        }
        self._send_last_unsuccessfull_time = {
            id: datetime.datetime(1, 1, 1) for id in self._all_ids if id != self._id
        }

    def __init__(self, id: int, all_ids: List[int]):
        self._id = id
        self._all_ids = sorted(all_ids)

        self._next_sequence_number = 1
        # delivered messages counters
        self._vector_clock = [0 for _ in range(len(all_ids))]

        self._message_states: Dict[MessageUid, MessageState] = {}
        self._commited_messages: set[MessageUid] = set()
        self._delivered_messages: deque[broadcast_pb2.ClientMessage] = deque()

        self._mutex = Lock()
        self._pending_requests_locks: Dict[int, Lock] = {}

        self._init_grpc()

        print("starting grpc server")
        self._grpc_server.start()

        self._workThread = Thread(target=self.work)
        print("starting work thread")
        self._workThread.start()

    def submit_message(self, msg: broadcast_pb2.ClientMessage, timeout=5) -> bool:
        self._mutex.acquire()
        print("client submitted a message")
        broadcast_msg = broadcast_pb2.BroadcastMessage()
        broadcast_msg.message.CopyFrom(msg)
        broadcast_msg.metadata.originId = self._id
        broadcast_msg.metadata.originSqnNum = self._next_sequence_number
        self._next_sequence_number += 1
        broadcast_msg.metadata.knownReplicatedOnHosts.append(self._id)
        # all messages delivered before current happen before it
        broadcast_msg.metadata.vectorClock.extend(self._vector_clock)

        for id in self._all_ids:
            if id != self._id:
                self._send_queues[id].append(broadcast_msg)
        msg_uid = _get_msg_uid(broadcast_msg)
        self._message_states[msg_uid] = MessageState(
            message=broadcast_msg, known_replicated_hosts=set([self._id])
        )
        self._pending_requests_locks[broadcast_msg.metadata.originSqnNum] = Lock()
        self._pending_requests_locks[broadcast_msg.metadata.originSqnNum].acquire()

        # client message is instantly delivered
        self.deliver(msg_uid, allow_deliver_self=True)
        self._mutex.release()

        # wait for actual delivery
        if self._pending_requests_locks[broadcast_msg.metadata.originSqnNum].acquire(
            timeout=timeout
        ):
            self._pending_requests_locks.pop(broadcast_msg.metadata.originSqnNum)
            return True
        else:
            # datarace?
            self._pending_requests_locks.pop(broadcast_msg.metadata.originSqnNum)
            return False

    def try_get_delivered(self):
        self._mutex.acquire()
        acquired = None
        if len(self._delivered_messages) > 0:
            print("client consumed a delivered message")
            acquired = self._delivered_messages.popleft()
        self._mutex.release()
        return acquired

    def handle_broadcast_message(self, broadcast_msg: broadcast_pb2.BroadcastMessage):
        self._mutex.acquire()
        print("incoming broadcast message")
        msg_uid = MessageUid(
            broadcast_msg.metadata.originId, broadcast_msg.metadata.originSqnNum
        )

        # TODO: handle stale messages, clear _message_states
        # if all([broadcast_msg.metadata.vectorClock[i] <= self._vector_clock[i] for i in range(len(self._all_ids))]):
        #     ind = self._all_ids.index(broadcast_msg.metadata.originId)
        #     if broadcast_msg.metadata.vectorClock[ind] < self._vector_clock[ind]:
        #         # broadcast has been received about already delivered message
        #         return

        is_first_time = not (msg_uid in self._message_states)

        if is_first_time:
            self._message_states[msg_uid] = MessageState(
                message=broadcast_msg, known_replicated_hosts=set()
            )

        old_replicated_hosts = self._message_states[msg_uid].known_replicated_hosts
        new_replicated_hosts = set(broadcast_msg.metadata.knownReplicatedOnHosts)
        new_replicated_hosts.add(self._id)
        new_replicated_hosts = new_replicated_hosts | old_replicated_hosts
        self._message_states[msg_uid].known_replicated_hosts = new_replicated_hosts

        if is_first_time:
            broadcast_msg.metadata.knownReplicatedOnHosts.extend(
                [
                    obj
                    for obj in self._message_states[msg_uid].known_replicated_hosts
                    if obj not in broadcast_msg.metadata.knownReplicatedOnHosts
                ]
            )
            ack_msg = broadcast_pb2.BroadcastMessage()
            ack_msg.CopyFrom(broadcast_msg)
            ack_msg.message.Clear()
            for id in self._all_ids:
                if id != self._id:
                    # Optimization: don't send message payload to node we know replicated it
                    if id in self._message_states[msg_uid].known_replicated_hosts:
                        self._send_queues[id].append(ack_msg)
                    else:
                        self._send_queues[id].append(broadcast_msg)

        if not len(old_replicated_hosts) * 2 > len(self._all_ids) and len(
            new_replicated_hosts
        ) * 2 > len(self._all_ids):
            self.commit(msg_uid)

        self._message_states[msg_uid].acks_received += 1
        if self._message_states[msg_uid].acks_received == len(self._all_ids):
            self._message_states.pop(msg_uid)

        self._mutex.release()

    def commit(self, msg_uid: MessageUid):
        print("message commited")
        self._commited_messages.add(msg_uid)
        changed = True
        while changed:
            changed = False
            for msg_uid in self._commited_messages:
                if all(
                    [
                        self._message_states[msg_uid].message.metadata.vectorClock[i]
                        <= self._vector_clock[i]
                        for i in range(len(self._all_ids))
                    ]
                ):
                    self.deliver(msg_uid)
                    self._commited_messages.remove(msg_uid)
                    changed = True
                    break

    def deliver(self, msg_uid: MessageUid, allow_deliver_self=False):
        msg = self._message_states[msg_uid].message
        if allow_deliver_self or msg.metadata.originId != self._id:
            print("message delivered")
            assert not self._message_states[msg_uid].is_delivered
            self._message_states[msg_uid].is_delivered = True

            self._delivered_messages.append(msg.message)
            self._vector_clock[self._all_ids.index(msg.metadata.originId)] += 1
        else:  # try unlock pending request
            print("finishing pending request")
            lock = self._pending_requests_locks.get(msg.metadata.originSqnNum, None)
            if not lock is None:
                lock.release()

    def work(self):
        while True:
            self._mutex.acquire()
            for id, queue in self._send_queues.items():
                if len(queue) > 0 and (
                    datetime.datetime.now() - self._send_last_unsuccessfull_time[id]
                ) > datetime.timedelta(seconds=5):
                    # print("sending message to node", id)
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
