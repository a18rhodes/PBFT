"""
Integration-style tests for the PBFT Node consensus logic.

These tests exercise the full protocol round-trips (PrePrepare → Prepare → Commit,
view change, and backup forwarding) without starting real HTTP servers, by patching
network I/O and timers at the seam.
"""

import json
import threading
from unittest.mock import MagicMock, patch

import pytest
import xxhash

from pbft.models.constants import Endpoints
from pbft.models.message import Commit, NewView, Prepare, PrePrepare, ViewChange
from pbft.models.node import Node

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPLICAS = ["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"]


def _digest(content) -> int:
    return xxhash.xxh64_intdigest(json.dumps(content).encode())


def _make_node(port: int, view: int = 0) -> Node:
    """Build a Node whose _id matches the given port on localhost."""
    with patch.dict("os.environ", {"HOST": "127.0.0.1", "PORT": str(port)}):
        return Node(view=view, replicas=list(REPLICAS))


def _no_broadcast(node: Node):
    """Patch _broadcast_to_peers so it records calls without real HTTP."""
    node._broadcast_to_peers = MagicMock()  # type: ignore[method-assign]
    return node


def _no_timer(node: Node):
    """Patch threading.Timer so timers never fire automatically."""
    node._start_timer_on_request = MagicMock()  # type: ignore[method-assign]
    return node


def _make_cluster(view: int = 0):
    """Return four patched nodes, primary first (index 0 for view 0)."""
    nodes = [_no_timer(_no_broadcast(_make_node(p, view))) for p in [8000, 8001, 8002, 8003]]
    return nodes


class TestNodeProperties:
    def test_primary_is_view_mod_n(self):
        node = _make_node(8000)
        assert node.primary_id == REPLICAS[0 % 4]

    def test_f_and_thresholds(self):
        node = _make_node(8000)
        assert node.f == 1
        assert node.prepared_threshold == 2
        assert node.quorum_threshold == 3

    def test_is_primary_true_for_port_8000_view_0(self):
        node = _make_node(8000)
        assert node.is_primary

    def test_is_primary_false_for_backup(self):
        node = _make_node(8001)
        assert not node.is_primary

    def test_replicas_returns_copy(self):
        node = _make_node(8000)
        replicas = node.replicas
        replicas.append("http://extra")
        assert len(node.replicas) == 4

    def test_log_returns_deep_copy(self):
        node = _make_node(8000)
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=1, content="x")
        node.log_message(pp)
        snap = node.log
        snap[(0, 0)].clear()
        assert len(node._log[(0, 0)]) == 1

    def test_next_sequence_increments(self):
        node = _make_node(8000)
        assert node.next_sequence() == 0
        assert node.next_sequence() == 1

    def test_add_remove_replica(self):
        node = _make_node(8000)
        node.add_replica("http://127.0.0.1:9000")
        assert "http://127.0.0.1:9000" in node.replicas
        node.remove_replica("http://127.0.0.1:9000")
        assert "http://127.0.0.1:9000" not in node.replicas

    def test_remove_nonexistent_replica_is_silent(self):
        node = _make_node(8000)
        node.remove_replica("http://nope")  # must not raise

    def test_replica_count(self):
        node = _make_node(8000)
        assert node.replica_count == 4


class TestFullCommitRoundTrip:
    def _run_round_trip(self):
        nodes = _make_cluster()
        primary, *backups = nodes
        content = "hello"
        digest = _digest(content)

        identity = primary.handle_new_request(content)
        view, seq = identity
        assert (view, seq) == (0, 0)

        pp_call = primary._broadcast_to_peers.call_args
        assert pp_call is not None
        pre_prepare = pp_call.kwargs["message"]
        assert isinstance(pre_prepare, PrePrepare)
        assert pre_prepare.client_request_digest == digest

        for backup in backups:
            backup.process_message(pre_prepare)
        prepare_messages = [backup._broadcast_to_peers.call_args.kwargs["message"] for backup in backups]
        for pm in prepare_messages:
            assert isinstance(pm, Prepare)

        for pm in prepare_messages:
            for node in nodes:
                node.process_message(pm)

        for node in nodes:
            assert node.prepared(item=pre_prepare)

        commit_messages = [
            call.kwargs["message"]
            for node in nodes
            for call in node._broadcast_to_peers.call_args_list
            if isinstance(call.kwargs.get("message"), Commit)
        ]

        for cm in commit_messages:
            for node in nodes:
                node.process_message(cm)

        for node in nodes:
            assert node.committed(item=pre_prepare)
        return identity

    def test_commit_round_trip(self):
        self._run_round_trip()

    def test_committed_status_via_tuple(self):
        nodes = _make_cluster()
        primary = nodes[0]
        content = "status-check"
        identity = primary.handle_new_request(content)
        pre_prepare = primary._broadcast_to_peers.call_args.kwargs["message"]
        for backup in nodes[1:]:
            backup.process_message(pre_prepare)
        prepare_messages = [n._broadcast_to_peers.call_args.kwargs["message"] for n in nodes[1:]]
        for pm in prepare_messages:
            for n in nodes:
                n.process_message(pm)
        commit_messages = [
            call.kwargs["message"]
            for n in nodes
            for call in n._broadcast_to_peers.call_args_list
            if isinstance(call.kwargs.get("message"), Commit)
        ]
        for cm in commit_messages:
            for n in nodes:
                n.process_message(cm)
        view, seq = identity
        assert nodes[0].committed((view, seq))


class TestPrimaryDeduplication:
    def test_duplicate_request_returns_same_identity(self):
        nodes = _make_cluster()
        primary = nodes[0]
        id1 = primary.handle_new_request("dup")
        id2 = primary.handle_new_request("dup")
        assert id1 == id2
        broadcast_count = sum(
            1
            for call in primary._broadcast_to_peers.call_args_list
            if isinstance(call.kwargs.get("message"), PrePrepare)
        )
        assert broadcast_count == 1


class TestBackupForwarding:
    def test_backup_forwards_to_primary_and_starts_timer(self):
        backup = _make_node(8001)
        backup._broadcast_to_peers = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = [0, 0]
        content = "forwarded"
        with patch("requests.post", return_value=mock_response) as mock_post:
            result = backup.handle_new_request(content)
        mock_post.assert_called_once()
        call_url = mock_post.call_args.args[0] if mock_post.call_args.args else mock_post.call_args.kwargs["url"]
        assert Endpoints.REQUEST in call_url
        assert result == [0, 0]


class TestViewMismatch:
    def test_preprepare_wrong_view_is_ignored(self):
        node = _no_broadcast(_make_node(8001))
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=99, sequence=0, client_request_digest=1, content="x")
        node.process_message(pp)
        assert (99, 0) not in node._log

    def test_prepare_wrong_view_is_ignored(self):
        node = _no_broadcast(_make_node(8001))
        pm = Prepare(id_="http://127.0.0.1:8000", view=99, sequence=0, client_request_digest=1)
        node.process_message(pm)
        assert node._log == {}

    def test_commit_wrong_view_returns_false(self):
        node = _no_broadcast(_make_node(8000))
        cm = Commit(id_="http://127.0.0.1:8000", view=99, sequence=0, client_request_digest=1)
        result = node.process_message(cm)
        assert result is False


class TestDuplicatePrePrepare:
    def test_second_preprepare_for_same_identity_is_dropped(self):
        node = _no_broadcast(_make_node(8001))
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=1, content="x")
        node.process_message(pp)
        node._broadcast_to_peers.reset_mock()
        node.process_message(pp)
        node._broadcast_to_peers.assert_not_called()


class TestTimerManagement:
    def test_timer_started_on_primary_request(self):
        node = _no_broadcast(_make_node(8000))
        mock_timer = MagicMock(spec=threading.Timer)
        with patch("threading.Timer", return_value=mock_timer) as mock_timer_cls:
            node.handle_new_request("timed")
        mock_timer_cls.assert_called_once()
        mock_timer.start.assert_called_once()

    def test_timer_cancelled_on_commit_quorum(self):
        nodes = _make_cluster()
        primary = nodes[0]
        content = "cancel-me"
        digest = _digest(content)
        real_timer = MagicMock(spec=threading.Timer)
        primary._timers[digest] = real_timer
        commit = Commit(id_="dummy", view=0, sequence=0, client_request_digest=digest)
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=digest, content=content)
        primary.log_message(pp)
        for i in range(primary.quorum_threshold):
            primary.log_message(Prepare(id_=f"id{i}", view=0, sequence=0, client_request_digest=digest))
        for i in range(primary.quorum_threshold):
            primary.log_message(Commit(id_=f"id{i}", view=0, sequence=0, client_request_digest=digest))
        primary._accept_commit(commit)
        real_timer.cancel.assert_called_once()

    def test_cancel_timer_returns_false_when_none(self):
        node = _make_node(8000)
        assert node._cancel_timer(99999) is False

    def test_start_timer_cancels_existing(self):
        node = _make_node(8000)
        existing = MagicMock(spec=threading.Timer)
        node._timers[42] = existing
        new_timer = MagicMock(spec=threading.Timer)
        with patch("threading.Timer", return_value=new_timer):
            node._start_timer_on_request(42)
        existing.cancel.assert_called_once()
        new_timer.start.assert_called_once()


class TestIssueViewChange:
    def test_view_change_broadcast_with_proofs(self):
        node = _no_broadcast(_make_node(8001))
        digest = _digest("vc-content")
        pp = PrePrepare(
            id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=digest, content="vc-content"
        )
        node.log_message(pp)
        for i in range(node.prepared_threshold):
            node.log_message(Prepare(id_=f"peer{i}", view=0, sequence=0, client_request_digest=digest))
        node.issue_view_change_request()
        vc = node._broadcast_to_peers.call_args.kwargs["message"]
        assert isinstance(vc, ViewChange)
        assert vc.view == 1
        assert len(vc.proofs) == 1

    def test_view_change_broadcast_without_proofs(self):
        node = _no_broadcast(_make_node(8001))
        node.issue_view_change_request()
        vc = node._broadcast_to_peers.call_args.kwargs["message"]
        assert isinstance(vc, ViewChange)
        assert vc.proofs == ()


class TestViewChangeProtocol:
    def _make_view_change(self, sender_id: str, proposed_view: int, proofs=()) -> ViewChange:
        return ViewChange(id_=sender_id, view=proposed_view, proofs=proofs)

    def test_new_primary_broadcasts_new_view_on_quorum(self):
        # view=1 → primary = replicas[1 % 4] = 8001
        nodes = _make_cluster(view=0)
        new_primary = nodes[1]
        new_primary.view = 0

        view_changes = [self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4)]
        for vc in view_changes:
            new_primary.process_view_change(vc)

        calls = [call.kwargs.get("message") for call in new_primary._broadcast_to_peers.call_args_list]
        new_view_msgs = [m for m in calls if isinstance(m, NewView)]
        assert len(new_view_msgs) == 1
        assert new_view_msgs[0].view == 1

    def test_non_primary_ignores_view_change_quorum(self):
        nodes = _make_cluster(view=0)
        non_primary = nodes[0]  # view=1 makes 8001 primary, not 8000
        view_changes = [self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4)]
        for vc in view_changes:
            non_primary.process_view_change(vc)
        new_view_calls = [
            call.kwargs.get("message")
            for call in non_primary._broadcast_to_peers.call_args_list
            if isinstance(call.kwargs.get("message"), NewView)
        ]
        assert new_view_calls == []

    def test_process_new_view_advances_view(self):
        node = _no_broadcast(_make_node(8000))
        view_changes = tuple(self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4))
        new_view = NewView(id_="http://127.0.0.1:8001", view=1, view_changes=view_changes, pre_prepares=())
        node.process_new_view(new_view)
        assert node.view == 1

    def test_process_new_view_ignores_stale_view(self):
        node = _no_broadcast(_make_node(8000))
        node.view = 5
        view_changes = tuple(self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4))
        new_view = NewView(id_="http://127.0.0.1:8001", view=1, view_changes=view_changes, pre_prepares=())
        node.process_new_view(new_view)
        assert node.view == 5

    def test_process_new_view_cancels_timers(self):
        node = _no_broadcast(_make_node(8000))
        timer = MagicMock(spec=threading.Timer)
        node._timers[99] = timer
        view_changes = tuple(self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4))
        new_view = NewView(id_="http://127.0.0.1:8001", view=1, view_changes=view_changes, pre_prepares=())
        node.process_new_view(new_view)
        timer.cancel.assert_called_once()

    def test_process_new_view_with_pre_prepares_reprocesses(self):
        node = _no_broadcast(_make_node(8000))
        digest = _digest("reprocess")
        pp = PrePrepare(
            id_="http://127.0.0.1:8001", view=1, sequence=0, client_request_digest=digest, content="reprocess"
        )
        view_changes = tuple(self._make_view_change(REPLICAS[i], proposed_view=1) for i in range(4))
        new_view = NewView(id_="http://127.0.0.1:8001", view=1, view_changes=view_changes, pre_prepares=(pp,))
        node.process_new_view(new_view)
        assert (1, 0) in node._log

    def test_view_change_insufficient_quorum_does_nothing(self):
        nodes = _make_cluster(view=0)
        new_primary = nodes[1]
        vc = self._make_view_change(REPLICAS[0], proposed_view=1)
        new_primary.process_view_change(vc)
        new_view_calls = [
            call.kwargs.get("message")
            for call in new_primary._broadcast_to_peers.call_args_list
            if isinstance(call.kwargs.get("message"), NewView)
        ]
        assert new_view_calls == []

    def test_view_change_updates_sequence_from_proofs(self):
        nodes = _make_cluster(view=0)
        new_primary = nodes[1]
        digest = _digest("seq-proof")
        pp_proof = PrePrepare(id_=REPLICAS[0], view=0, sequence=7, client_request_digest=digest, content="seq-proof")
        prepare_proofs = tuple(
            Prepare(id_=REPLICAS[i], view=0, sequence=7, client_request_digest=digest) for i in range(2)
        )
        proofs = ((pp_proof, prepare_proofs),)
        view_changes = [self._make_view_change(REPLICAS[i], proposed_view=1, proofs=proofs) for i in range(4)]
        for vc in view_changes:
            new_primary.process_view_change(vc)
        assert new_primary._sequence == 8


class TestCheckQuorum:
    def test_unknown_type_raises(self):
        node = _make_node(8000)
        with pytest.raises(NotImplementedError):
            node._check_quorum("not-a-valid-type", threshold=1, message_type=Prepare)

    def test_quorum_by_tuple_identity(self):
        node = _make_node(8000)
        digest = _digest("q")
        pp = PrePrepare(id_="x", view=0, sequence=0, client_request_digest=digest, content="q")
        node.log_message(pp)
        for i in range(2):
            node.log_message(Prepare(id_=f"p{i}", view=0, sequence=0, client_request_digest=digest))
        assert node.prepared((0, 0))

    def test_process_message_unknown_type_raises(self):
        node = _make_node(8000)

        class Alien:
            pass

        with pytest.raises(NotImplementedError):
            node.process_message(Alien())


class TestFireAndForget:
    def test_fire_and_forget_suppresses_request_exception(self):
        import requests as req

        with patch("requests.request", side_effect=req.exceptions.ConnectionError):
            Node._fire_and_forget(method="POST", url="http://nowhere", json={})


class TestBroadcastToPeers:
    def test_broadcast_calls_fire_and_forget_for_each_replica(self):
        node = _make_node(8000)
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=1, content="b")
        with patch.object(Node, "_fire_and_forget") as mock_ff:
            node._broadcast_to_peers(method="POST", message=pp, endpoint=Endpoints.MESSAGE)
        assert mock_ff.call_count == len(REPLICAS)
        for replica in REPLICAS:
            mock_ff.assert_any_call(method="POST", url=replica + Endpoints.MESSAGE, json=pp.model_dump())
