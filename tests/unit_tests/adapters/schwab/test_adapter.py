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
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import InstrumentId


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


@pytest.mark.asyncio
async def test_subscribe_sends_schwab_request_envelope() -> None:
    sent: list[dict[str, Any]] = []
    client = SchwabWebSocketClient(
        clock=StubClock(),
        http_client=SimpleNamespace(),
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
        http_client=SimpleNamespace(),
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

    order_spec = SchwabExecutionClient._add_order_leg(client, OrderBuilder(), order).build()

    leg = order_spec["orderLegCollection"][0]
    assert leg["instruction"] == "BUY_TO_OPEN"
    assert leg["instrument"]["assetType"] == "OPTION"
    assert leg["instrument"]["symbol"] == "AAPL240119C00150000"
