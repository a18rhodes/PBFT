"""
Tests for the PBFT CLI commands.

Uses click.testing.CliRunner to invoke commands in-process and mocks out
all external I/O (uvicorn, subprocess, requests) so no real servers start.
"""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pbft.cli import cli
from pbft.models.constants import Endpoints


@pytest.fixture()
def runner():
    return CliRunner()


class TestLaunchNode:
    def test_launches_uvicorn_with_defaults(self, runner):
        with patch("pbft.cli.uvicorn.run") as mock_run:
            result = runner.invoke(cli, ["launch-node"])
        assert result.exit_code == 0
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs["port"] == 8000
        assert kwargs["host"] == "127.0.0.1"

    def test_launches_uvicorn_with_custom_port_and_host(self, runner):
        with patch("pbft.cli.uvicorn.run") as mock_run:
            result = runner.invoke(cli, ["launch-node", "--port", "9999", "--host", "0.0.0.0"])
        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["port"] == 9999
        assert kwargs["host"] == "0.0.0.0"

    def test_strips_http_prefix_from_host(self, runner):
        with patch("pbft.cli.uvicorn.run") as mock_run:
            result = runner.invoke(cli, ["launch-node", "--host", "http://127.0.0.1"])
        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["host"] == "127.0.0.1"


class TestLaunchCluster:
    def test_spawns_correct_number_of_processes(self, runner):
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        with patch("pbft.cli.subprocess.Popen", return_value=mock_proc) as mock_popen:
            result = runner.invoke(cli, ["launch-cluster", "--port-min", "8000", "--num-nodes", "4"])
        assert result.exit_code == 0
        assert mock_popen.call_count == 4

    def test_kills_processes_on_exception(self, runner):
        mock_proc = MagicMock()
        mock_proc.wait.side_effect = RuntimeError("boom")
        with patch("pbft.cli.subprocess.Popen", return_value=mock_proc):
            runner.invoke(cli, ["launch-cluster", "--port-min", "8000", "--num-nodes", "2"])
        assert mock_proc.kill.call_count == 2


class TestSend:
    def test_send_reports_committed(self, runner):
        post_resp = MagicMock()
        post_resp.json.return_value = [0, 0]
        get_resp = MagicMock()
        get_resp.json.return_value = {"status": True}
        with (
            patch("pbft.cli.requests.post", return_value=post_resp),
            patch("pbft.cli.requests.get", return_value=get_resp),
        ):
            result = runner.invoke(cli, ["send", "hello", "--port", "8000"])
        assert "valid" in result.output
        assert "not" not in result.output

    def test_send_reports_not_committed(self, runner):
        post_resp = MagicMock()
        post_resp.json.return_value = [0, 0]
        get_resp = MagicMock()
        get_resp.json.return_value = {"status": False}
        with (
            patch("pbft.cli.requests.post", return_value=post_resp),
            patch("pbft.cli.requests.get", return_value=get_resp),
        ):
            result = runner.invoke(cli, ["send", "hello", "--port", "8000"])
        assert "not valid" in result.output

    def test_send_handles_invalid_response(self, runner):
        post_resp = MagicMock()
        post_resp.json.return_value = "unexpected"
        get_resp = MagicMock()
        get_resp.json.return_value = {}
        with (
            patch("pbft.cli.requests.post", return_value=post_resp),
            patch("pbft.cli.requests.get", return_value=get_resp),
        ):
            result = runner.invoke(cli, ["send", "hello", "--port", "8000"])
        assert "invalid response" in result.output

    def test_send_broadcasts_to_multiple_ports(self, runner):
        post_resp = MagicMock()
        post_resp.json.return_value = [0, 0]
        get_resp = MagicMock()
        get_resp.json.return_value = {"status": True}
        with (
            patch("pbft.cli.requests.post", return_value=post_resp) as mock_post,
            patch("pbft.cli.requests.get", return_value=get_resp),
        ):
            runner.invoke(cli, ["send", "hello", "--port", "8000", "--port", "8001", "--port", "8002"])
        assert mock_post.call_count == 3


class TestVerify:
    def test_verify_calls_status_endpoint(self, runner):
        with patch("pbft.cli.requests.get") as mock_get:
            runner.invoke(cli, ["verify", "hello"])
        mock_get.assert_called_once()
        assert Endpoints.STATUS in mock_get.call_args.args[0]
