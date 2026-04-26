# Copyright 2024 Google LLC
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
from __future__ import annotations

from typing import Any, Callable, Dict, Type, Optional, Union

from google.protobuf.message import Message
from google.protobuf.internal.enum_type_wrapper import EnumTypeWrapper
from google.cloud.bigtable.data.execute_query.values import Struct
from google.cloud.bigtable.data.execute_query.metadata import SqlType
from google.cloud.bigtable_v2 import Value as PBValue
from google.api_core.datetime_helpers import DatetimeWithNanoseconds

_REQUIRED_PROTO_FIELDS = {
    SqlType.Bytes: "bytes_value",
    SqlType.String: "string_value",
    SqlType.Int64: "int_value",
    SqlType.Float32: "float_value",
    SqlType.Float64: "float_value",
    SqlType.Bool: "bool_value",
    SqlType.Timestamp: "timestamp_value",
    SqlType.Date: "date_value",
    SqlType.Struct: "array_value",
    SqlType.Array: "array_value",
    SqlType.Map: "array_value",
    SqlType.Proto: "bytes_value",
    SqlType.Enum: "int_value",
}


def _parse_array_type(
    value: PBValue,
    metadata_type: SqlType.Array,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> list[Any]:
    """
    used for parsing an array represented as a protobuf to a python list.
    """
    pass


def _parse_map_type(
    value: PBValue,
    metadata_type: SqlType.Map,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> dict[Any, Any]:
    """
    used for parsing a map represented as a protobuf to a python dict.

    Values of type `Map` are stored in a `Value.array_value` where each entry
    is another `Value.array_value` with two elements (the key and the value,
    in that order).
    Normally encoded Map values won't have repeated keys, however, the client
    must handle the case in which they do. If the same key appears
    multiple times, the _last_ value takes precedence.
    """
    pass


def _parse_struct_type(
    value: PBValue,
    metadata_type: SqlType.Struct,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> Struct:
    """
    used for parsing a struct represented as a protobuf to a
    google.cloud.bigtable.data.execute_query.Struct
    """
    pass


def _parse_timestamp_type(
    value: PBValue,
    metadata_type: SqlType.Timestamp,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> DatetimeWithNanoseconds:
    """
    used for parsing a timestamp represented as a protobuf to DatetimeWithNanoseconds
    """
    pass


def _parse_proto_type(
    value: PBValue,
    metadata_type: SqlType.Proto,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> Message | bytes:
    """
    Parses a serialized protobuf message into a Message object using type information
    provided in column_info.

    Args:
        value: The value to parse, expected to have a bytes_value attribute.
        metadata_type: The expected SQL type (Proto).
        column_name: The name of the column.
        column_info: (Optional) A dictionary mapping column names to their
            corresponding Protobuf Message classes. This information is used
            to deserialize the raw bytes.

    Returns:
        A deserialized Protobuf Message object if parsing is successful.
        If the required type information is not found in column_info, the function
        returns the original serialized data as bytes (value.bytes_value).
        This fallback ensures that the raw data is still accessible.

    Raises:
        google.protobuf.message.DecodeError: If `value.bytes_value` cannot be
            parsed as the Message type specified in `column_info`.
    """
    pass


def _parse_enum_type(
    value: PBValue,
    metadata_type: SqlType.Enum,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> int | str:
    """
    Parses an integer value into a Protobuf enum name string using type information
    provided in column_info.

    Args:
        value: The value to parse, expected to have an int_value attribute.
        metadata_type: The expected SQL type (Enum).
        column_name: The name of the column.
        column_info: (Optional) A dictionary mapping column names to their
            corresponding Protobuf EnumTypeWrapper objects. This information
            is used to convert the integer to an enum name.

    Returns:
        A string representing the name of the enum value if conversion is successful.
        If conversion fails for any reason, such as the required EnumTypeWrapper
        not being found in column_info, or if an error occurs during the name lookup
        (e.g., the integer is not a valid enum value), the function returns the
        original integer value (value.int_value). This fallback ensures the
        raw integer representation is still accessible.
    """
    pass


ParserCallable = Callable[
    [PBValue, Any, Optional[str], Optional[Dict[str, Union[Message, EnumTypeWrapper]]]],
    Any,
]

_TYPE_PARSERS: Dict[Type[SqlType.Type], ParserCallable] = {
    SqlType.Timestamp: _parse_timestamp_type,
    SqlType.Struct: _parse_struct_type,
    SqlType.Array: _parse_array_type,
    SqlType.Map: _parse_map_type,
    SqlType.Proto: _parse_proto_type,
    SqlType.Enum: _parse_enum_type,
}


def _parse_pb_value_to_python_value(
    value: PBValue,
    metadata_type: SqlType.Type,
    column_name: str | None,
    column_info: dict[str, Message | EnumTypeWrapper] | None = None,
) -> Any:
    """
    used for converting the value represented as a protobufs to a python object.
    """
    value_kind = value.WhichOneof("kind")
    if not value_kind:
        return None

    kind = type(metadata_type)
    if not value.HasField(_REQUIRED_PROTO_FIELDS[kind]):
        raise ValueError(
            f"{_REQUIRED_PROTO_FIELDS[kind]} field for {kind.__name__} type not found in a Value."
        )

    if kind in _TYPE_PARSERS:
        parser = _TYPE_PARSERS[kind]
        return parser(value, metadata_type, column_name, column_info)
    elif kind in _REQUIRED_PROTO_FIELDS:
        field_name = _REQUIRED_PROTO_FIELDS[kind]
        return getattr(value, field_name)
    else:
        raise ValueError(f"Unknown kind {kind}")
