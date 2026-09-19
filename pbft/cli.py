"""
Human-facing CLI commands for launching clusters, sending requests,
and verifying committed messages.
"""

__all__ = ["cli"]

import os
import subprocess

import click
import requests
import uvicorn

from pbft.app import app
from pbft.models.constants import Endpoints

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8000

_host_option = click.option(
    "--host",
    "-H",
    envvar="HOST",
    default=_DEFAULT_HOST,
    type=str,
    help="Host for the node",
    callback=lambda ctx, param, v: v.removeprefix("http://"),
)


@click.group(context_settings={"help_option_names": ["-h", "--help"], "show_default": True})
def cli(): ...


@cli.command()
@_host_option
@click.option("--port", "-p", envvar="PORT", default=_DEFAULT_PORT, type=int, help="Port for the node")
def launch_node(port, host):
    uvicorn.run(app=app, port=port, host=host)


@cli.command()
@_host_option
@click.option(
    "--port-min",
    "-m",
    envvar="PORT_MIN",
    default=_DEFAULT_PORT,
    type=int,
    help="Starting port; one node is launched per port in [port-min, port-min + num-nodes).",
)
@click.option("--num-nodes", "-n", type=int, help="Number of replica nodes")
def launch_cluster(port_min, num_nodes, host):
    port_max = port_min + num_nodes
    env = os.environ.copy()
    env.update({"REPLICAS": ",".join(f"http://{host}:{port}" for port in range(port_min, port_max))})
    processes: list[subprocess.Popen] = []  # type: ignore[annotation-unchecked]
    try:
        for port in range(port_min, port_max):
            env.update({"PORT": str(port), "HOST": host})
            processes.append(subprocess.Popen(["uv", "run", "pbft", "launch-node"], env=env))
        for process in processes:
            process.wait()
    except Exception, KeyboardInterrupt:
        for process in processes:
            process.kill()
        raise


@cli.command()
@click.argument("message", type=str)
@_host_option
@click.option("--port", "-p", "ports", type=int, help="Port for the node", multiple=True)
def send(message, ports, host):
    for port in ports:
        response = requests.post(f"http://{host}:{port}" + Endpoints.REQUEST, json=message)
    data = response.json()
    if isinstance(data, (list, tuple)):
        view, id_ = data
        valid = requests.get(f"http://{host}:{port}{Endpoints.STATUS}/{view}/{id_}").json().get("status", False)
        click.echo(f"Message is{'' if valid else ' not'} valid. ({view=}, id={id_})")
    else:
        click.echo(f"invalid response type.... {data}, {type(data)}")


@cli.command()
@click.argument("message", type=str)
@_host_option
@click.option("--port", "-p", envvar="PORT", default=_DEFAULT_PORT, type=int, help="Port for any node")
def verify(message, port, host):
    requests.get(f"http://{host}:{port}" + Endpoints.STATUS)


if __name__ == "__main__":
    cli()
