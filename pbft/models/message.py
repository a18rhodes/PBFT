"""
Pydantic message models for the PBFT protocol.

The discriminated union MessageRequest enables FastAPI to deserialize any
inbound message to the correct concrete type via the message_type field.
"""

__all__ = ["Commit", "Message", "MessageRequest", "NewView", "PrePrepare", "Prepare", "RequestMessage", "ViewChange"]

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class Message(BaseModel):
    model_config = ConfigDict(frozen=True)
    id_: str
    view: int = Field(ge=0, default=0)


class RequestMessage(Message):
    """All request-based messages"""

    sequence: int = Field(ge=0, default=0)
    client_request_digest: int

    @property
    def identity(self) -> tuple[int, int]:
        return (self.view, self.sequence)


class PrePrepare(RequestMessage):
    """PrePrepare message type"""

    content: Any
    message_type: Literal["PrePrepare"] = "PrePrepare"


class Prepare(RequestMessage):
    """Prepare message type"""

    message_type: Literal["Prepare"] = "Prepare"


class Commit(RequestMessage):
    """Commit message type"""

    message_type: Literal["Commit"] = "Commit"


class ViewChange(Message):
    """View change request"""

    proofs: tuple[tuple[PrePrepare, tuple[Prepare, ...]], ...]


class NewView(Message):
    """New view message type"""

    view_changes: tuple[ViewChange, ...]
    pre_prepares: tuple[PrePrepare, ...]


MessageRequest = Annotated[Commit | Prepare | PrePrepare, Field(discriminator="message_type")]
