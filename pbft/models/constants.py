"""
Endpoint path constants for inter-replica HTTP communication.
"""

__all__ = ["Endpoints"]

from enum import StrEnum


class Endpoints(StrEnum):
    MESSAGE = "/message"
    NEW_VIEW = "/new-view"
    PRIMARY = "/primary"
    REPLICA = "/replica"
    REQUEST = "/request"
    STATUS = "/status"
    VIEW_CHANGE = "/view-change"
