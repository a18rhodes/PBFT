"""
HTTP transport layer for the PBFT protocol.

Exposes endpoints for client requests, inter-replica messaging,
replica membership management, and commit status queries.
"""

__all__ = ["app", "node"]

import os

from fastapi import Body, Depends, FastAPI

from pbft.models.constants import Endpoints
from pbft.models.message import MessageRequest, NewView, ViewChange
from pbft.models.node import Node

app = FastAPI()
node = Node(view=0, replicas=os.getenv("REPLICAS", "").split(","))


@app.post(Endpoints.MESSAGE)
def receive_message(message: MessageRequest, state: Node = Depends(lambda: node)):
    state.process_message(message)


@app.post(Endpoints.REQUEST)
def make_request(content: str = Body(), state: Node = Depends(lambda: node)):
    return state.handle_new_request(content=content)


@app.put(Endpoints.REPLICA)
def register_replica(address: str, state: Node = Depends(lambda: node)):
    state.add_replica(address)


@app.delete(Endpoints.REPLICA)
def remove_replica(address: str, state: Node = Depends(lambda: node)):
    state.remove_replica(address)


@app.get(Endpoints.STATUS + "/{view}/{sequence}")
def get_status(view: int, sequence: int, state: Node = Depends(lambda: node)) -> dict[str, bool]:
    return {"status": state.committed((view, sequence))}


@app.get(Endpoints.PRIMARY)
def get_primary(state: Node = Depends(lambda: node)):
    return state.primary_id


@app.post(Endpoints.VIEW_CHANGE)
def change_view(view_change: ViewChange, state: Node = Depends(lambda: node)):
    state.process_view_change(message=view_change)


@app.post(Endpoints.NEW_VIEW)
def new_veiw(new_view: NewView, state: Node = Depends(lambda: node)):
    state.process_new_view(message=new_view)
