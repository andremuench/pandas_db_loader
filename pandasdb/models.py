from enum import Enum, auto


class ColumnRole(Enum):
    TRACK_INSERT = auto()
    TRACK_UPDATE = auto()


class MergePart(Enum):
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()

    @classmethod
    def all(cls):
        return set([MergePart.INSERT, MergePart.UPDATE, MergePart.DELETE])
