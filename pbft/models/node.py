"""
Core PBFT consensus logic.

The Node class drives the three-phase commit protocol (PrePrepare → Prepare → Commit),
manages view-change timeouts, and handles leader election during failover.
"""

__all__ = ["Node"]

import bisect
import contextlib
import json
import os
import threading
from copy import deepcopy
from functools import singledispatchmethod
from typing import Any

import requests
import xxhash

from pbft.models.constants import Endpoints
from pbft.models.message import Commit, Message, NewView, Prepare, PrePrepare, RequestMessage, ViewChange


class Node:
    _TIMEOUT = 8

    def __init__(self, view: int, replicas: list[str]):
        self._id = f"http://{os.getenv('HOST', '127.0.0.1')}:{os.getenv('PORT', '8000')}"
        self.view = view
        self._sequence = 0
        self._log: dict[tuple[int, int], list[RequestMessage]] = {}
        self._replicas = sorted(replicas)
        self._timers: dict[int, threading.Timer] = {}
        self._lock = threading.Lock()
        self._view_change_requests: dict[int, list[ViewChange]] = {}

    def next_sequence(self) -> int:
        """
        Assigns the next available sequence number to a new PrePrepare.

        :return: The current sequence counter, post-increment.
        """
        current = self._sequence
        self._sequence += 1
        return current

    @property
    def replicas(self) -> list[str]:
        return list(self._replicas)

    def add_replica(self, replica: str):
        """
        Inserts a replica into the sorted membership list.

        :param replica: The base URL of the replica to add.
        """
        bisect.insort(self._replicas, replica)

    def remove_replica(self, replica: str):
        """
        Removes a replica from the membership list.

        :param replica: The base URL of the replica to remove.
        """
        with contextlib.suppress(ValueError):
            self._replicas.remove(replica)

    @property
    def primary_id(self) -> str:
        return self.replicas[self.view % self.replica_count]

    @property
    def is_primary(self) -> bool:
        return self._id == self.primary_id

    @property
    def replica_count(self) -> int:
        return len(self.replicas)

    @property
    def f(self) -> int:
        return (self.replica_count - 1) // 3

    @property
    def prepared_threshold(self) -> int:
        return 2 * self.f

    @property
    def quorum_threshold(self) -> int:
        return self.prepared_threshold + 1

    @property
    def log(self) -> dict[tuple[int, int], list[RequestMessage]]:
        """
        A deep copy of the message log, safe for external inspection.

        :return: A snapshot of the current log state.
        """
        return deepcopy(self._log)

    def prepared(self, item: Message | tuple) -> bool:
        """
        Returns True if a prepared quorum (2f Prepares) exists for item.

        :param item: A message or (view, sequence) identity tuple.
        """
        return self._check_quorum(item, threshold=self.prepared_threshold, message_type=Prepare)

    def committed(self, item: Message | tuple) -> bool:
        """
        Returns True if the prepared condition holds and a commit quorum (2f+1 Commits) exists.

        :param item: A message or (view, sequence) identity tuple.
        """
        return self.prepared(item=item) and self._check_quorum(
            item, threshold=self.quorum_threshold, message_type=Commit
        )

    def log_message(self, message: RequestMessage):
        """
        Appends a message to the log under its (view, sequence) identity.

        :param message: The message to record.
        """
        self._log.setdefault(message.identity, []).append(message)

    def _start_timer_on_request(self, client_request_digest: int):
        with self._lock:
            self._cancel_timer(client_request_digest)
            timer = threading.Timer(self._TIMEOUT, self.issue_view_change_request)
            timer.daemon = True
            timer.start()
            self._timers[client_request_digest] = timer

    @staticmethod
    def _fire_and_forget(**kwargs):
        with contextlib.suppress(requests.exceptions.RequestException):
            requests.request(**kwargs)

    def _broadcast_to_peers(self, method: str, message: Message, endpoint: str):
        for replica in self.replicas:
            self._fire_and_forget(method=method, url=replica + endpoint, json=message.model_dump())

    def _disarm_timer_on_message(self, message: Commit):
        with self._lock:
            self._cancel_timer(message.client_request_digest)

    def _cancel_timer(self, client_request_digest: int) -> bool:
        if timer := self._timers.pop(client_request_digest, None):
            timer.cancel()
            return True
        return False

    def handle_new_request(self, content: Any) -> tuple[int, int]:
        """
        Initiates consensus for a new client request.

        Primary nodes assign a sequence number and broadcast a PrePrepare, skipping
        duplicates already present in the log. Backup nodes forward to the primary
        and start a failover timer once the primary has acknowledged.

        :param content: The client-supplied payload to commit.
        :return: The (view, sequence) identity assigned to this request.
        """
        client_request_digest = xxhash.xxh64_intdigest(json.dumps(content).encode())
        if not self.is_primary:
            primary_response = requests.post(self.primary_id + Endpoints.REQUEST, json=content).json()
            self._start_timer_on_request(client_request_digest=client_request_digest)
            return primary_response
        for messages in self._log.values():
            for msg in messages:
                if isinstance(msg, PrePrepare) and msg.client_request_digest == client_request_digest:
                    return msg.identity
        message = PrePrepare(
            id_=self._id,
            view=self.view,
            sequence=self.next_sequence(),
            client_request_digest=client_request_digest,
            content=content,
        )
        self._start_timer_on_request(client_request_digest=client_request_digest)
        self.log_message(message=message)
        self._broadcast_to_peers(method="POST", message=message, endpoint=Endpoints.MESSAGE)
        return message.identity

    def process_new_view(self, message: NewView):
        """
        Adopts a new view and re-processes any prepared requests carried in it.

        :param message: The NewView message from the incoming primary.
        """
        if message.view > self.view and len(message.view_changes) >= self.quorum_threshold:
            for timer in self._timers.values():
                timer.cancel()
            self.view = message.view
            for pre_prepare in message.pre_prepares:
                self.process_message(pre_prepare)

    def process_view_change(self, message: ViewChange):
        """
        Accumulates ViewChange votes and, if this node becomes the new primary,
        broadcasts a NewView once a quorum is reached.

        :param message: An incoming ViewChange from a peer.
        """
        self._view_change_requests.setdefault(message.view, []).append(message)
        if not self._proposed_view_makes_self_primary(message):
            return
        if len(self._view_change_requests.get(message.view, [])) < self.quorum_threshold:
            return
        new_pre_prepares = self._get_new_preprepares_for_view_change(message=message)
        self._sequence = max([pp.sequence for pp in new_pre_prepares] or [0]) + 1
        new_view = NewView(
            id_=self._id,
            view=message.view,
            view_changes=tuple(self._view_change_requests[message.view]),
            pre_prepares=new_pre_prepares,
        )
        self._view_change_requests.pop(message.view, None)
        self._broadcast_to_peers(method="POST", message=new_view, endpoint=Endpoints.NEW_VIEW)

    def _get_new_preprepares_for_view_change(self, message: ViewChange) -> tuple[PrePrepare, ...]:
        new_pre_prepares = []
        for view_change in self._view_change_requests.get(message.view, []):
            for proof in view_change.proofs:
                pre_prepare = proof[0]
                new_pre_prepares.append(
                    PrePrepare(
                        id_=self._id,
                        content=pre_prepare.content,
                        view=message.view,
                        sequence=pre_prepare.sequence,
                        client_request_digest=pre_prepare.client_request_digest,
                    )
                )
        return tuple(new_pre_prepares)

    def _proposed_view_makes_self_primary(self, view_change: ViewChange) -> bool:
        return self._replicas[view_change.view % self.replica_count] == self._id

    @singledispatchmethod
    def process_message(self, message: Message):
        """
        Routes an incoming protocol message to its type-specific handler.

        :param message: A PrePrepare, Prepare, or Commit.
        """
        raise NotImplementedError(f"Cannot handle message of type {type(message)}")

    @process_message.register
    def _validate_pre_prepare(self, message: PrePrepare):
        if self.view != message.view:
            return
        if not any(isinstance(thing, PrePrepare) for thing in self._log.get(message.identity, [])):
            self.log_message(message=message)
            prepare = Prepare(
                id_=self._id,
                view=message.view,
                sequence=message.sequence,
                client_request_digest=message.client_request_digest,
            )
            self._broadcast_to_peers(method="POST", message=prepare, endpoint=Endpoints.MESSAGE)

    @process_message.register
    def _accept_prepare(self, message: Prepare):
        if self.view != message.view:
            return
        self.log_message(message=message)
        if self.prepared(item=message):
            commit = Commit(
                id_=self._id,
                view=message.view,
                sequence=message.sequence,
                client_request_digest=message.client_request_digest,
            )
            self._broadcast_to_peers(method="POST", message=commit, endpoint=Endpoints.MESSAGE)

    @process_message.register
    def _accept_commit(self, message: Commit) -> bool:
        if self.view != message.view:
            return False
        self.log_message(message)
        if self.committed(item=message):
            self._disarm_timer_on_message(message=message)
            return True
        return False

    @singledispatchmethod
    def _check_quorum(self, item: Any, threshold: int, message_type: type[RequestMessage]) -> bool:
        raise NotImplementedError(f"Cannot check quorum of type {type(item)}")

    @_check_quorum.register
    def _cq_message(self, item: RequestMessage, threshold: int, message_type: type[RequestMessage]) -> bool:
        types = [type(message_) for message_ in self._log.get(item.identity, [])]
        return types.count(message_type) >= threshold

    @_check_quorum.register
    def _cq_identity(self, item: tuple, threshold: int, message_type: type[RequestMessage]) -> bool:
        types = [type(message_) for message_ in self._log.get(item, [])]
        return types.count(message_type) >= threshold

    def issue_view_change_request(self):
        """
        Broadcasts a ViewChange to all replicas, proposing the next view.

        Called when the failover timer fires, indicating the current primary
        is unresponsive.
        """
        proposed_view = self.view + 1
        proofs = []
        for messages in self._log.values():
            by_type = {}
            for message in messages:
                by_type.setdefault(type(message), []).append(message)
            if len(by_type.get(Prepare, [])) >= self.prepared_threshold:
                proofs.append((by_type[PrePrepare][0], tuple(by_type[Prepare])))
        view_change = ViewChange(id_=self._id, view=proposed_view, proofs=tuple(proofs))
        self._broadcast_to_peers(method="POST", message=view_change, endpoint=Endpoints.VIEW_CHANGE)
