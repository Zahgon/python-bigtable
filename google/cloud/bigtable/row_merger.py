from enum import Enum
from collections import OrderedDict
from google.cloud.bigtable.row import Cell, PartialRowData, InvalidChunk

_MISSING_COLUMN_FAMILY = "Column family {} is not among the cells stored in this row."
_MISSING_COLUMN = (
    "Column {} is not among the cells stored in this row in the column family {}."
)
_MISSING_INDEX = (
    "Index {!r} is not valid for the cells stored in this row for column {} "
    "in the column family {}. There are {} such cells."
)


class _State(Enum):
    ROW_START = "ROW_START"
    CELL_START = "CELL_START"
    CELL_IN_PROGRESS = "CELL_IN_PROGRESS"
    CELL_COMPLETE = "CELL_COMPLETE"
    ROW_COMPLETE = "ROW_COMPLETE"


class _PartialRow(object):
    __slots__ = [
        "row_key",
        "cells",
        "last_family",
        "last_family_cells",
        "last_qualifier",
        "last_qualifier_cells",
        "cell",
    ]

    def __init__(self, row_key):
        self.row_key = row_key
        self.cells = OrderedDict()

        self.last_family = None
        self.last_family_cells = OrderedDict()
        self.last_qualifier = None
        self.last_qualifier_cells = []

        self.cell = None


class _PartialCell(object):
    __slots__ = ["family", "qualifier", "timestamp", "labels", "value", "value_index"]

    def __init__(self):
        self.family = None
        self.qualifier = None
        self.timestamp = None
        self.labels = None
        self.value = None
        self.value_index = 0


class _RowMerger(object):
    """
    State machine to merge chunks from a response stream into logical rows.

    The implementation is a fairly linear state machine that is implemented as
    a method for every state in the _State enum. In general the states flow
    from top to bottom with some repetition. Each state handler will do some
    sanity checks, update in progress data and set the next state.

    There can be multiple state transitions for each chunk, i.e. a single chunk
    row will flow from ROW_START -> CELL_START -> CELL_COMPLETE -> ROW_COMPLETE
    in a single iteration.
    """

    __slots__ = ["state", "last_seen_row_key", "row"]

    def __init__(self, last_seen_row=b""):
        self.last_seen_row_key = last_seen_row
        self.state = _State.ROW_START
        self.row = None

    def process_chunks(self, response):
        """
        Process the chunks in the given response and yield logical rows.
        This class will maintain state across multiple response protos.
        """
        pass

    def _handle_reset(self, chunk):
        pass

    def _handle_row_start(self, chunk):
        pass

    def _handle_cell_start(self, chunk):
        # Ensure that all chunks after the first one either are missing a row
        # key or the row is the same
        pass

    def _handle_cell_in_progress(self, chunk):
        # if this isn't the first cell chunk, make sure that everything except
        # the value stayed constant.
        pass

    def _handle_cell_complete(self, chunk):
        # since we are guaranteed that all family & qualifier cells are
        # contiguous, we can optimize away the dict lookup by caching the last
        # family/qualifier and simply comparing and appending
        pass

    def _handle_row_complete(self, chunk):
        pass

    def finalize(self):
        """
        Must be called at the end of the stream to ensure there are no unmerged
        rows.
        """
        pass
