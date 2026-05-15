# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
from types import SimpleNamespace
from typing import Any
from typing import cast

import httpx
import pytest
from schwab.orders.common import OptionInstruction
from schwab.orders.generic import OrderBuilder

from nautilus_trader.adapters.schwab.data import SchwabDataClient
from nautilus_trader.adapters.schwab.execution import SCHWAB_STATUS_MAP
from nautilus_trader.adapters.schwab.execution import SchwabExecutionClient
from nautilus_trader.adapters.schwab.http.client import SchwabHttpClientError
from nautilus_trader.adapters.schwab.http.error import should_retry
from nautilus_trader.adapters.schwab.websocket.client import SchwabWebSocketClient
from nautilus_trader.live.retry import RetryManagerPool
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import VenueOrderId


class StubClock:
    def timestamp_ns(self) -> int:
        return 0


class StubWebSocket:
    def __init__(self) -> None:
        self.disconnects = 0
        self.closed = False

    def is_closed(self) -> bool:
        return self.closed

    async def disconnect(self) -> None:
        self.disconnects += 1
        self.closed = True


class StubLogger:
    def debug(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:
        pass


@pytest.mark.asyncio
async def test_subscribe_sends_schwab_request_envelope() -> None:
    sent: list[dict[str, Any]] = []
    client = SchwabWebSocketClient(
        clock=StubClock(),
        http_client=cast(Any, SimpleNamespace()),
        handler=None,
        handler_reconnect=None,
        loop=asyncio.get_running_loop(),
    )

    async def capture(msg: dict[str, Any]) -> None:
        sent.append(msg)

    client._send = capture  # type: ignore[method-assign]

    await client._subscribe("AAPL", "LEVELONE_EQUITIES", "SUBS")

    assert len(sent) == 1
    assert list(sent[0]) == ["requests"]
    assert sent[0]["requests"][0]["service"] == "LEVELONE_EQUITIES"
    assert sent[0]["requests"][0]["parameters"]["keys"] == "AAPL"


@pytest.mark.asyncio
async def test_disconnect_closes_transport_when_last_reference_disconnects() -> None:
    transport = StubWebSocket()
    client = SchwabWebSocketClient(
        clock=StubClock(),
        http_client=cast(Any, SimpleNamespace()),
        handler=None,
        handler_reconnect=None,
        loop=asyncio.get_running_loop(),
    )
    client._client = transport  # type: ignore[assignment]
    client._connect_ref_count = 2
    client._subscriptions = {"LEVELONE_EQUITIES": ["AAPL"]}

    await client.disconnect()

    assert transport.disconnects == 0
    assert client._client is transport

    await client.disconnect()

    assert transport.disconnects == 1
    assert client._client is None
    assert client._subscriptions == {}


def test_book_level_volume_prefers_total_volume() -> None:
    level = {
        "TOTAL_VOLUME": 275,
        "BIDS": [
            {"BID_VOLUME": 100},
            {"BID_VOLUME": 175},
        ],
    }

    assert (
        SchwabDataClient._book_level_volume(
            level,
            total_volume_key="TOTAL_VOLUME",
            exchange_levels_key="BIDS",
            exchange_volume_key="BID_VOLUME",
        )
        == 275.0
    )


def test_book_level_volume_sums_per_exchange_volumes() -> None:
    level = {
        "ASKS": [
            {"ASK_VOLUME": 100},
            {"ASK_VOLUME": "175"},
        ],
    }

    assert (
        SchwabDataClient._book_level_volume(
            level,
            total_volume_key="TOTAL_VOLUME",
            exchange_levels_key="ASKS",
            exchange_volume_key="ASK_VOLUME",
        )
        == 275.0
    )


def test_accepted_status_maps_to_accepted() -> None:
    assert SCHWAB_STATUS_MAP["ACCEPTED"] == OrderStatus.ACCEPTED


def test_should_retry_schwab_http_client_retryable_status() -> None:
    assert should_retry(SchwabHttpClientError("rate limited", status_code=429))
    assert should_retry(SchwabHttpClientError("server error", status_code=503))
    assert not should_retry(SchwabHttpClientError("bad request", status_code=400))


def test_should_retry_httpx_status_error() -> None:
    request = httpx.Request("GET", "https://api.schwabapi.com/test")
    response = httpx.Response(503, request=request)
    error = httpx.HTTPStatusError("server error", request=request, response=response)

    assert should_retry(error)


def test_add_order_leg_uses_option_leg_for_option_orders() -> None:
    client = SimpleNamespace(
        _is_option_order=lambda order: True,
        _get_option_action=lambda order: OptionInstruction.BUY_TO_OPEN,
        _get_order_action=lambda order: None,
    )
    order = SimpleNamespace(
        instrument_id=InstrumentId.from_str("AAPL240119C00150000.SCHWAB"),
        quantity=1,
    )

    order_spec = SchwabExecutionClient._add_order_leg(
        cast(Any, client),
        OrderBuilder(),
        cast(Any, order),
    ).build()

    leg = order_spec["orderLegCollection"][0]
    assert leg["instruction"] == "BUY_TO_OPEN"
    assert leg["instrument"]["assetType"] == "OPTION"
    assert leg["instrument"]["symbol"] == "AAPL240119C00150000"


@pytest.mark.asyncio
async def test_submit_order_does_not_retry_whole_submission_on_retryable_error() -> None:
    request = httpx.Request("GET", "https://api.schwabapi.com/test")
    response = httpx.Response(503, request=request)
    error = httpx.HTTPStatusError("server error", request=request, response=response)
    submit_calls = 0
    rejected_events: list[dict[str, Any]] = []

    async def submit(order: Any) -> None:
        nonlocal submit_calls
        submit_calls += 1
        raise error

    client = SimpleNamespace(
        _client_order_to_instrument={},
        _client_order_to_venue={},
        _venue_order_to_client={},
        _submit_order_methods={OrderType.LIMIT: submit},
        _clock=StubClock(),
        generate_order_submitted=lambda **kwargs: None,
        generate_order_rejected=lambda **kwargs: rejected_events.append(kwargs),
    )
    order = SimpleNamespace(
        order_type=OrderType.LIMIT,
        venue_order_id=None,
        client_order_id=ClientOrderId("C-001"),
        strategy_id="S-001",
        instrument_id=InstrumentId.from_str("AAPL.SCHWAB"),
    )
    command = SimpleNamespace(order=order)

    await SchwabExecutionClient._submit_order(cast(Any, client), cast(Any, command))

    assert submit_calls == 1
    assert len(rejected_events) == 1


@pytest.mark.asyncio
async def test_submit_reports_accepted_without_replacing_order_when_status_lookup_fails() -> None:
    place_calls = 0
    accepted_events: list[dict[str, Any]] = []

    class StubHttpClient:
        async def place_order(self, account_hash: str, order_spec: dict[str, Any]) -> str:
            nonlocal place_calls
            place_calls += 1
            return "12345"

    async def get_order_status_after_submit(venue_order_id: str) -> None:
        return None

    client = SimpleNamespace(
        _http_client=StubHttpClient(),
        _account_hash="acct",
        _client_order_to_venue={},
        _venue_order_to_client={},
        _get_order_status_after_submit=get_order_status_after_submit,
        _log=StubLogger(),
        _clock=StubClock(),
        generate_order_accepted=lambda **kwargs: accepted_events.append(kwargs),
    )
    order = SimpleNamespace(
        is_post_only=False,
        client_order_id=ClientOrderId("C-001"),
        strategy_id="S-001",
        instrument_id=InstrumentId.from_str("AAPL.SCHWAB"),
    )

    await SchwabExecutionClient._submit_and_check_order(cast(Any, client), cast(Any, order), {})

    assert place_calls == 1
    assert client._client_order_to_venue[order.client_order_id] == VenueOrderId("12345")
    assert len(accepted_events) == 1
    assert accepted_events[0]["venue_order_id"] == VenueOrderId("12345")


@pytest.mark.asyncio
async def test_get_order_status_after_submit_retries_status_lookup() -> None:
    request = httpx.Request("GET", "https://api.schwabapi.com/test")
    response = httpx.Response(503, request=request)

    class StubHttpClient:
        def __init__(self) -> None:
            self.calls = 0

        async def get_order(self, order_id: str, account_hash: str) -> dict[str, Any]:
            self.calls += 1
            if self.calls < 3:
                raise httpx.HTTPStatusError(
                    "server error",
                    request=request,
                    response=response,
                )
            return {"status": "WORKING"}

    http_client = StubHttpClient()
    client = SimpleNamespace(
        _http_client=http_client,
        _account_hash="acct",
        _retry_manager_pool=RetryManagerPool[None](
            pool_size=1,
            max_retries=2,
            delay_initial_ms=1,
            delay_max_ms=1,
            backoff_factor=1,
            logger=StubLogger(),
            exc_types=(SchwabHttpClientError, httpx.HTTPStatusError),
            retry_check=should_retry,
        ),
        _log=StubLogger(),
    )

    result = await SchwabExecutionClient._get_order_status_after_submit(cast(Any, client), "12345")

    assert result == {"status": "WORKING"}
    assert http_client.calls == 3
