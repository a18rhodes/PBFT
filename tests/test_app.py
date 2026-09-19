"""
HTTP endpoint tests for the PBFT FastAPI application.

Uses TestClient to exercise the full request/response cycle without
starting a real server, and dependency-overrides to inject a controlled
Node state for each test.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
import xxhash
from fastapi.testclient import TestClient

import pbft.app as app_module
from pbft.app import app
from pbft.models.message import Commit, NewView, Prepare, PrePrepare, ViewChange
from pbft.models.node import Node

REPLICAS = ["http://127.0.0.1:8000", "http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"]


def _fresh_node() -> Node:
    with patch.dict("os.environ", {"HOST": "127.0.0.1", "PORT": "8000"}):
        n = Node(view=0, replicas=list(REPLICAS))
    n._broadcast_to_peers = MagicMock()  # type: ignore[method-assign]
    n._start_timer_on_request = MagicMock()  # type: ignore[method-assign]
    return n


@pytest.fixture()
def client():
    controlled = _fresh_node()
    original = app_module.node
    app_module.node = controlled
    yield TestClient(app), controlled
    app_module.node = original


class TestMessageEndpoint:
    def test_post_preprepare(self, client):
        tc, _ = client
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=1, content="x")
        resp = tc.post("/message", json=pp.model_dump())
        assert resp.status_code == 200

    def test_post_prepare(self, client):
        tc, _ = client
        pm = Prepare(id_="http://127.0.0.1:8001", view=0, sequence=0, client_request_digest=1)
        resp = tc.post("/message", json=pm.model_dump())
        assert resp.status_code == 200

    def test_post_commit(self, client):
        tc, _ = client
        cm = Commit(id_="http://127.0.0.1:8002", view=0, sequence=0, client_request_digest=1)
        resp = tc.post("/message", json=cm.model_dump())
        assert resp.status_code == 200


class TestRequestEndpoint:
    def test_primary_handles_request(self, client):
        tc, _ = client
        resp = tc.post("/request", json="hello")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2


class TestReplicaEndpoint:
    def test_put_adds_replica(self, client):
        tc, state = client
        resp = tc.put("/replica", params={"address": "http://127.0.0.1:9000"})
        assert resp.status_code == 200
        assert "http://127.0.0.1:9000" in state.replicas

    def test_delete_removes_replica(self, client):
        tc, state = client
        tc.put("/replica", params={"address": "http://127.0.0.1:9000"})
        resp = tc.delete("/replica", params={"address": "http://127.0.0.1:9000"})
        assert resp.status_code == 200
        assert "http://127.0.0.1:9000" not in state.replicas


class TestStatusEndpoint:
    def test_not_committed_returns_false(self, client):
        tc, _ = client
        resp = tc.get("/status/0/0")
        assert resp.status_code == 200
        assert resp.json() == {"status": False}

    def test_committed_returns_true(self, client):
        tc, state = client
        digest = xxhash.xxh64_intdigest(json.dumps("stat").encode())
        pp = PrePrepare(id_="http://127.0.0.1:8000", view=0, sequence=0, client_request_digest=digest, content="stat")
        state.log_message(pp)
        for i in range(state.quorum_threshold):
            state.log_message(Prepare(id_=f"p{i}", view=0, sequence=0, client_request_digest=digest))
        for i in range(state.quorum_threshold):
            state.log_message(Commit(id_=f"c{i}", view=0, sequence=0, client_request_digest=digest))
        resp = tc.get("/status/0/0")
        assert resp.json() == {"status": True}


class TestPrimaryEndpoint:
    def test_returns_primary_id(self, client):
        tc, state = client
        resp = tc.get("/primary")
        assert resp.status_code == 200
        assert resp.json() == state.primary_id


class TestViewChangeEndpoint:
    def test_post_view_change(self, client):
        tc, _ = client
        vc = ViewChange(id_="http://127.0.0.1:8001", view=1, proofs=())
        resp = tc.post("/view-change", json=vc.model_dump())
        assert resp.status_code == 200


class TestNewViewEndpoint:
    def test_post_new_view_advances_state(self, client):
        tc, state = client
        view_changes = tuple(ViewChange(id_=REPLICAS[i], view=1, proofs=()) for i in range(4))
        nv = NewView(id_="http://127.0.0.1:8001", view=1, view_changes=view_changes, pre_prepares=())
        resp = tc.post("/new-view", json=nv.model_dump())
        assert resp.status_code == 200
        assert state.view == 1
