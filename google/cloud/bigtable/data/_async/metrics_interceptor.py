# Copyright 2025 Google LLC
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

from typing import Sequence

import time
from functools import wraps

from google.cloud.bigtable.data._metrics.data_model import ActiveOperationMetric
from google.cloud.bigtable.data._metrics.data_model import OperationState
from google.cloud.bigtable.data._metrics.data_model import OperationType

from google.cloud.bigtable.data._cross_sync import CrossSync

if CrossSync.is_async:
    from grpc.aio import UnaryUnaryClientInterceptor
    from grpc.aio import UnaryStreamClientInterceptor
    from grpc.aio import AioRpcError
else:
    from grpc import UnaryUnaryClientInterceptor
    from grpc import UnaryStreamClientInterceptor


__CROSS_SYNC_OUTPUT__ = "google.cloud.bigtable.data._sync_autogen.metrics_interceptor"


def _with_active_operation(func):
    """
    Decorator for interceptor methods to extract the active operation associated with the
    in-scope contextvars, and pass it to the decorated function.
    """
    pass


@CrossSync.convert
async def _get_metadata(source) -> dict[str, str | bytes] | None:
    """Helper to extract metadata from a call or RpcError"""
    pass


@CrossSync.convert_class(sync_name="BigtableMetricsInterceptor")
class AsyncBigtableMetricsInterceptor(
    UnaryUnaryClientInterceptor, UnaryStreamClientInterceptor
):
    """
    An async gRPC interceptor to add client metadata and print server metadata.
    """

    @CrossSync.convert
    @_with_active_operation
    async def intercept_unary_unary(
        self, operation, continuation, client_call_details, request
    ):
        """
        Interceptor for unary rpcs:
          - MutateRow
          - CheckAndMutateRow
          - ReadModifyWriteRow
        """
        pass

    @CrossSync.convert
    @_with_active_operation
    async def intercept_unary_stream(
        self, operation, continuation, client_call_details, request
    ):
        """
        Interceptor for streaming rpcs:
          - ReadRows
          - MutateRows
          - SampleRowKeys
        """
        pass

    @staticmethod
    @CrossSync.convert
    async def _streaming_generator_wrapper(operation, call):
        """
        Wrapped generator to be returned by intercept_unary_stream.
        """
        pass
