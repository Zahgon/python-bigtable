# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations
from typing import TYPE_CHECKING, Any
from bisect import bisect_left
from bisect import bisect_right
from collections import defaultdict
from google.cloud.bigtable.data.row_filters import RowFilter

from google.cloud.bigtable_v2.types import RowRange as RowRangePB
from google.cloud.bigtable_v2.types import RowSet as RowSetPB
from google.cloud.bigtable_v2.types import ReadRowsRequest as ReadRowsRequestPB

if TYPE_CHECKING:
    from google.cloud.bigtable.data import RowKeySamples
    from google.cloud.bigtable.data import ShardedQuery


class RowRange:
    """
    Represents a range of keys in a ReadRowsQuery

    Args:
        start_key: The start key of the range. If empty, the range is unbounded on the left.
        end_key: The end key of the range. If empty, the range is unbounded on the right.
        start_is_inclusive: Whether the start key is inclusive. If None, the start key is
            inclusive.
        end_is_inclusive: Whether the end key is inclusive. If None, the end key is not inclusive.
    Raises:
        ValueError: if start_key is greater than end_key, or start_is_inclusive
        ValueError: if end_is_inclusive is set when the corresponding key is None
        ValueError: if start_key or end_key is not a string or bytes.
    """

    __slots__ = ("_pb",)

    def __init__(
        self,
        start_key: str | bytes | None = None,
        end_key: str | bytes | None = None,
        start_is_inclusive: bool | None = None,
        end_is_inclusive: bool | None = None,
    ):
        # convert empty key inputs to None for consistency
        start_key = None if not start_key else start_key
        end_key = None if not end_key else end_key
        # check for invalid combinations of arguments
        if start_is_inclusive is None:
            start_is_inclusive = True

        if end_is_inclusive is None:
            end_is_inclusive = False
        # ensure that start_key and end_key are bytes
        if isinstance(start_key, str):
            start_key = start_key.encode()
        elif start_key is not None and not isinstance(start_key, bytes):
            raise ValueError("start_key must be a string or bytes")
        if isinstance(end_key, str):
            end_key = end_key.encode()
        elif end_key is not None and not isinstance(end_key, bytes):
            raise ValueError("end_key must be a string or bytes")
        # ensure that start_key is less than or equal to end_key
        if start_key is not None and end_key is not None and start_key > end_key:
            raise ValueError("start_key must be less than or equal to end_key")

        init_dict = {}
        if start_key is not None:
            if start_is_inclusive:
                init_dict["start_key_closed"] = start_key
            else:
                init_dict["start_key_open"] = start_key
        if end_key is not None:
            if end_is_inclusive:
                init_dict["end_key_closed"] = end_key
            else:
                init_dict["end_key_open"] = end_key
        self._pb = RowRangePB(**init_dict)

    @property
    def start_key(self) -> bytes | None:
        """
        Returns the start key of the range. If None, the range is unbounded on the left.
        """
        pass

    @property
    def end_key(self) -> bytes | None:
        """
        Returns the end key of the range. If None, the range is unbounded on the right.

        Returns:
            bytes | None: The end key of the range, or None if the range is unbounded on the right.
        """
        pass

    @property
    def start_is_inclusive(self) -> bool:
        """
        Indicates if the range is inclusive of the start key.

        If the range is unbounded on the left, this will return True.

        Returns:
            bool: Whether the range is inclusive of the start key.
        """
        pass

    @property
    def end_is_inclusive(self) -> bool:
        """
        Indicates if the range is inclusive of the end key.

        If the range is unbounded on the right, this will return True.

        Returns:
            bool: Whether the range is inclusive of the end key.
        """
        pass

    def _to_pb(self) -> RowRangePB:
        """
        Converts this object to a protobuf

        Returns:
            RowRangePB: The protobuf representation of this object
        """
        return self._pb

    @classmethod
    def _from_pb(cls, data: RowRangePB) -> RowRange:
        """
        Creates a RowRange from a protobuf

        Args:
            data (RowRangePB): The protobuf to convert
        Returns:
            RowRange: The converted RowRange
        """
        instance = cls()
        instance._pb = data
        return instance

    @classmethod
    def _from_dict(cls, data: dict[str, bytes | str]) -> RowRange:
        """
        Creates a RowRange from a protobuf

        Args:
            data (dict[str, bytes | str]): The dictionary to convert
        Returns:
            RowRange: The converted RowRange
        """
        pass

    def __bool__(self) -> bool:
        """
        Empty RowRanges (representing a full table scan) are falsy, because
        they can be substituted with None. Non-empty RowRanges are truthy.

        Returns:
            bool: True if the RowRange is not empty, False otherwise
        """
        return bool(
            self._pb.start_key_closed
            or self._pb.start_key_open
            or self._pb.end_key_closed
            or self._pb.end_key_open
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RowRange):
            return NotImplemented
        return self._pb == other._pb

    def __str__(self) -> str:
        """
        Represent range as a string, e.g. "[b'a', b'z)"

        Unbounded start or end keys are represented as "-inf" or "+inf"

        Returns:
            str: The string representation of the range
        """
        left = "[" if self.start_is_inclusive else "("
        right = "]" if self.end_is_inclusive else ")"
        start = repr(self.start_key) if self.start_key is not None else "-inf"
        end = repr(self.end_key) if self.end_key is not None else "+inf"
        return f"{left}{start}, {end}{right}"

    def __repr__(self) -> str:
        args_list = []
        args_list.append(f"start_key={self.start_key!r}")
        args_list.append(f"end_key={self.end_key!r}")
        if self.start_is_inclusive is False:
            # only show start_is_inclusive if it is different from the default
            args_list.append(f"start_is_inclusive={self.start_is_inclusive}")
        if self.end_is_inclusive is True and self.end_key is not None:
            # only show end_is_inclusive if it is different from the default
            args_list.append(f"end_is_inclusive={self.end_is_inclusive}")
        return f"RowRange({', '.join(args_list)})"


class ReadRowsQuery:
    """
    Class to encapsulate details of a read row request

    Args:
        row_keys: row keys to include in the query
            a query can contain multiple keys, but ranges should be preferred
        row_ranges: ranges of rows to include in the query
        limit: the maximum number of rows to return. None or 0 means no limit
            default: None (no limit)
        row_filter: a RowFilter to apply to the query
    """

    slots = ("_limit", "_filter", "_row_set")

    def __init__(
        self,
        row_keys: list[str | bytes] | str | bytes | None = None,
        row_ranges: list[RowRange] | RowRange | None = None,
        limit: int | None = None,
        row_filter: RowFilter | None = None,
    ):
        if row_keys is None:
            row_keys = []
        if row_ranges is None:
            row_ranges = []
        if not isinstance(row_ranges, list):
            row_ranges = [row_ranges]
        if not isinstance(row_keys, list):
            row_keys = [row_keys]
        row_keys = [key.encode() if isinstance(key, str) else key for key in row_keys]
        self._row_set = RowSetPB(
            row_keys=row_keys, row_ranges=[r._pb for r in row_ranges]
        )
        self.limit = limit or None
        self.filter = row_filter

    @property
    def row_keys(self) -> list[bytes]:
        """
        Return the row keys in this query

        Returns:
            list[bytes]: the row keys in this query
        """
        pass

    @property
    def row_ranges(self) -> list[RowRange]:
        """
        Return the row ranges in this query

        Returns:
            list[RowRange]: the row ranges in this query
        """
        pass

    @property
    def limit(self) -> int | None:
        """
        Return the maximum number of rows to return by this query

        None or 0 means no limit

        Returns:
            int | None: the maximum number of rows to return by this query
        """
        pass

    @limit.setter
    def limit(self, new_limit: int | None):
        """
        Set the maximum number of rows to return by this query.

        None or 0 means no limit

        Args:
            new_limit: the new limit to apply to this query
        Raises:
            ValueError: if new_limit is < 0
        """
        pass

    @property
    def filter(self) -> RowFilter | None:
        """
        Return the RowFilter applied to this query

        Returns:
            RowFilter | None: the RowFilter applied to this query
        """
        pass

    @filter.setter
    def filter(self, row_filter: RowFilter | None):
        """
        Set a RowFilter to apply to this query

        Args:
            row_filter: a RowFilter to apply to this query
        """
        pass

    def add_key(self, row_key: str | bytes):
        """
        Add a row key to this query

        A query can contain multiple keys, but ranges should be preferred

        Args:
            row_key: a key to add to this query
        Raises:
            ValueError: if an input is not a string or bytes
        """
        pass

    def add_range(
        self,
        row_range: RowRange,
    ):
        """
        Add a range of row keys to this query.

        Args:
            row_range: a range of row keys to add to this query
        """
        pass

    def shard(self, shard_keys: RowKeySamples) -> ShardedQuery:
        """
        Split this query into multiple queries that can be evenly distributed
        across nodes and run in parallel

        Args:
            shard_keys: a list of row keys that define the boundaries of segments.
        Returns:
            ShardedQuery: a ShardedQuery that can be used in sharded_read_rows calls
        Raises:
            AttributeError: if the query contains a limit
        """
        pass

    @staticmethod
    def _shard_range(
        orig_range: RowRange, split_points: list[bytes]
    ) -> list[tuple[int, RowRange]]:
        """
        Helper function for sharding row_range into subranges that fit into
        segments of the key-space, determined by split_points

        Args:
            orig_range: a row range to split
            split_points: a list of row keys that define the boundaries of segments.
                each point represents the inclusive end of a segment
        Returns:
            list[tuple[int, RowRange]]: a list of tuples, containing a segment index and a new sub-range.
        """
        pass

    def _to_pb(self, table) -> ReadRowsRequestPB:
        """
        Convert this query into a dictionary that can be used to construct a
        ReadRowsRequest protobuf
        """
        return ReadRowsRequestPB(
            app_profile_id=table.app_profile_id,
            filter=self.filter._to_pb() if self.filter else None,
            rows_limit=self.limit or 0,
            rows=self._row_set,
            **table._request_path,
        )

    def __eq__(self, other):
        """
        RowRanges are equal if they have the same row keys, row ranges,
        filter and limit, or if they both represent a full scan with the
        same filter and limit

        Args:
            other: the object to compare to
        Returns:
            bool: True if the objects are equal, False otherwise
        """
        if not isinstance(other, ReadRowsQuery):
            return False
        # empty queries are equal
        if len(self.row_keys) == 0 and len(other.row_keys) == 0:
            this_range_empty = len(self.row_ranges) == 0 or all(
                [bool(r) is False for r in self.row_ranges]
            )
            other_range_empty = len(other.row_ranges) == 0 or all(
                [bool(r) is False for r in other.row_ranges]
            )
            if this_range_empty and other_range_empty:
                return self.filter == other.filter and self.limit == other.limit
        # otherwise, sets should have same sizes
        if len(self.row_keys) != len(other.row_keys):
            return False
        if len(self.row_ranges) != len(other.row_ranges):
            return False
        ranges_match = all([row in other.row_ranges for row in self.row_ranges])
        return (
            self.row_keys == other.row_keys
            and ranges_match
            and self.filter == other.filter
            and self.limit == other.limit
        )

    def __repr__(self):
        return f"ReadRowsQuery(row_keys={list(self.row_keys)}, row_ranges={list(self.row_ranges)}, row_filter={self.filter}, limit={self.limit})"
