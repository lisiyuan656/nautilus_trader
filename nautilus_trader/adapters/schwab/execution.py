# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
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
from __future__ import annotations

import asyncio
import copy
import json
from collections import deque
from collections.abc import Mapping
from contextlib import suppress
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from httpx import HTTPStatusError
from msgspec import json as msgspec_json
from schwab.orders.common import Duration as SchwabDuration
from schwab.orders.common import EquityInstruction
from schwab.orders.common import OptionInstruction
from schwab.orders.common import OrderStrategyType
from schwab.orders.common import OrderType as SchwabOrderType
from schwab.orders.common import PriceLinkBasis
from schwab.orders.common import PriceLinkType
from schwab.orders.common import Session as SchwabSession
from schwab.orders.generic import OrderBuilder
from schwab.streaming import StreamClient

from nautilus_trader.adapters.schwab.common import SCHWAB_VENUE
from nautilus_trader.adapters.schwab.common import parse_opra_symbol
from nautilus_trader.adapters.schwab.config import SchwabExecClientConfig
from nautilus_trader.adapters.schwab.http.client import SchwabHttpClient
from nautilus_trader.adapters.schwab.http.client import SchwabHttpClientError
from nautilus_trader.adapters.schwab.http.error import SchwabError
from nautilus_trader.adapters.schwab.http.error import should_retry
from nautilus_trader.adapters.schwab.providers import SchwabInstrumentProvider
from nautilus_trader.adapters.schwab.websocket.client import SchwabWebSocketClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.live.retry import RetryManagerPool
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import InstrumentClass
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.model.orders import TrailingStopLimitOrder
from nautilus_trader.model.orders import TrailingStopMarketOrder


ORDER_TYPE_MAP = {
    OrderType.MARKET: SchwabOrderType.MARKET,
    OrderType.LIMIT: SchwabOrderType.LIMIT,
    OrderType.STOP_MARKET: SchwabOrderType.STOP,
    OrderType.STOP_LIMIT: SchwabOrderType.STOP_LIMIT,
    OrderType.TRAILING_STOP_MARKET: SchwabOrderType.TRAILING_STOP,
    OrderType.TRAILING_STOP_LIMIT: SchwabOrderType.TRAILING_STOP_LIMIT,
}

ORDER_TYPE_REVERSE = {value: key for key, value in ORDER_TYPE_MAP.items()}

TIME_IN_FORCE_MAP = {
    TimeInForce.DAY: SchwabDuration.DAY,
    TimeInForce.GTC: SchwabDuration.GOOD_TILL_CANCEL,
    TimeInForce.IOC: SchwabDuration.IMMEDIATE_OR_CANCEL,
    TimeInForce.FOK: SchwabDuration.FILL_OR_KILL,
}

TIME_IN_FORCE_REVERSE = {value: key for key, value in TIME_IN_FORCE_MAP.items()}

ORDER_ACTION_MAP = {
    OrderSide.BUY: EquityInstruction.BUY,
    OrderSide.SELL: EquityInstruction.SELL,
}

TRAILING_OFFSET_TYPE_MAP = {
    TrailingOffsetType.PRICE: PriceLinkType.VALUE,
    TrailingOffsetType.BASIS_POINTS: PriceLinkType.PERCENT,
    TrailingOffsetType.TICKS: PriceLinkType.TICK,
}

TRAILING_OFFSET_TYPE_REVERSE = {value: key for key, value in TRAILING_OFFSET_TYPE_MAP.items()}

# TODO: schwab has no specific default here, need to double check
TRIGGER_TYPE_MAP = {
    TriggerType.LAST_PRICE: PriceLinkBasis.LAST,
    TriggerType.BID_ASK: PriceLinkBasis.ASK_BID,
    TriggerType.MARK_PRICE: PriceLinkBasis.MARK,
    TriggerType.MID_POINT: PriceLinkBasis.AVERAGE,
    TriggerType.DEFAULT: PriceLinkBasis.LAST,
}

SCHWAB_STATUS_MAP = {
    "ACCEPTED": OrderStatus.ACCEPTED,
    "WORKING": OrderStatus.SUBMITTED,
    "QUEUED": OrderStatus.SUBMITTED,
    "PENDING_ACTIVATION": OrderStatus.SUBMITTED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
}


class SchwabExecutionClient(LiveExecutionClient):
    """
    Execution client for Schwab brokerage accounts.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        http_client: SchwabHttpClient,
        instrument_provider: SchwabInstrumentProvider,
        config: SchwabExecClientConfig,
        name: str | None = None,
        ws_client: SchwabWebSocketClient | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or f"{SCHWAB_VENUE.value}-EXEC"),
            venue=SCHWAB_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.MARGIN,
            base_currency=USD,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )

        self._http_client = http_client
        self._config = config
        self._client_order_to_venue: dict[ClientOrderId, VenueOrderId] = {}
        self._client_order_to_instrument: dict[ClientOrderId, InstrumentId] = {}
        self._venue_order_to_client: dict[VenueOrderId, ClientOrderId] = {}
        self._open_orders: dict[VenueOrderId, Mapping[str, Any]] = {}
        self._account_hash: str | None = None

        if ws_client:
            self._ws_client = ws_client
            self._ws_client.register_handler(self._handle_ws_message)
            self._ws_client.register_reconnect_handler(self._handle_ws_reconnected)
        else:
            self._ws_client = SchwabWebSocketClient(
                clock=clock,
                http_client=http_client,
                handler=self._handle_ws_message,
                handler_reconnect=self._handle_ws_reconnected,
                loop=loop,
            )
        self._ws_connected = False
        self._recent_stream_trade_ids: deque[str] = deque()
        self._recent_stream_trade_ids_set: set[str] = set()
        self._recent_stream_trade_ids_limit = 2_048

        account_id_value = config.account_number

        if account_id_value:
            self._set_account_id(
                AccountId(f"{SCHWAB_VENUE.value}-{account_id_value}"),
            )
        else:
            self._set_account_id(AccountId(f"{SCHWAB_VENUE.value}-001"))

        self._retry_manager_pool = RetryManagerPool[None](
            pool_size=100,
            max_retries=config.max_retries or 0,
            delay_initial_ms=config.retry_delay_initial_ms or 1_000,
            delay_max_ms=config.retry_delay_max_ms or 10_000,
            backoff_factor=2,
            logger=self._log,
            exc_types=(SchwabError, SchwabHttpClientError, HTTPStatusError),
            retry_check=should_retry,
        )

        self._submit_order_methods = {
            OrderType.MARKET: self._submit_market_order,
            OrderType.LIMIT: self._submit_limit_order,
            OrderType.STOP_MARKET: self._submit_stop_market_order,
            OrderType.STOP_LIMIT: self._submit_stop_limit_order,
            OrderType.TRAILING_STOP_MARKET: self._submit_trailing_stop_market_order,
            OrderType.TRAILING_STOP_LIMIT: self._submit_trailing_stop_limit_order,
        }

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        await self._update_account_state()
        await self._ensure_account_activity_stream()

    async def _update_account_state(self) -> None:
        if self._account_hash is None:
            account_hashmap = await self._http_client.get_account_numbers()
            self._account_hash = account_hashmap[self._config.account_number]

        balances, margins = await self._http_client.get_account(
            self._account_hash,
            self.base_currency,
        )
        self.generate_account_state(
            balances=balances,
            margins=margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
        )

    async def _ensure_account_activity_stream(self) -> None:
        if self._ws_connected:
            return
        try:
            await self._ws_client.connect()
            await self._ws_client.subscribe_account_activity()
        except Exception as exc:
            self._log.warning(
                "Failed to initialize Schwab account activity stream",
                exc_info=exc,
            )
            return
        self._ws_connected = True

    async def _handle_ws_reconnected(self) -> None:
        try:
            await self._ws_client.subscribe_account_activity(force=True)
        except Exception as exc:
            self._log.warning(
                "Failed to resubscribe Schwab account activity stream",
                exc_info=exc,
            )

    async def _disconnect(self) -> None:
        await self._ws_client.disconnect()
        self._ws_connected = False
        self._log.info("Schwab execution client disconnected", LogColor.BLUE)

    # -- COMMAND HANDLERS -------------------------------------------------------------------------

    async def _submit_order(self, command: SubmitOrder) -> None:
        order = command.order

        self._client_order_to_instrument[order.client_order_id] = order.instrument_id

        if order.venue_order_id is not None:
            self._client_order_to_venue[order.client_order_id] = order.venue_order_id
            self._venue_order_to_client[order.venue_order_id] = order.client_order_id

        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            await retry_manager.run(
                "submit_order",
                [order.client_order_id],
                self._submit_order_methods[order.order_type],
                order,
            )

            if not retry_manager.result:
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=retry_manager.message,
                    ts_event=self._clock.timestamp_ns(),
                )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _submit_and_check_order(self, order: Order, order_spec: Mapping[str, Any]) -> None:
        if order.is_post_only:
            raise ValueError("`post_only` not supported by Schwab")
        venue_order_id = await self._http_client.place_order(self._account_hash, order_spec)

        if venue_order_id:
            venue_order = VenueOrderId(venue_order_id)
            self._client_order_to_venue[order.client_order_id] = venue_order
            self._venue_order_to_client[venue_order] = order.client_order_id
            order_status = await self._http_client.get_order(venue_order_id, self._account_hash)
            if order_status["status"] in [
                "WORKING",
                "PENDING_ACTIVATION",
                "FILLED",
                "QUEUED",
                "ACCEPTED",
            ]:
                self.generate_order_accepted(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=venue_order,
                    ts_event=self._clock.timestamp_ns(),
                )

                if order_status["status"] == "FILLED":
                    fills = self._extract_fills(order_status)
                    instrument = self._cache.instrument(order.instrument_id)
                    for fill in fills:
                        report = self._build_fill_report(
                            fill,
                            instrument=instrument,
                            instrument_id=order.instrument_id,
                            venue_order_id=venue_order,
                            client_order_id=order.client_order_id,
                            order_side=order.side,  # Use original order side as fallback/context
                        )
                        if report:
                            self.generate_order_filled(
                                strategy_id=order.strategy_id,
                                instrument_id=order.instrument_id,
                                client_order_id=order.client_order_id,
                                venue_order_id=venue_order,
                                venue_position_id=None,
                                trade_id=report.trade_id,
                                order_side=report.order_side,
                                order_type=order.order_type,
                                last_qty=report.last_qty,
                                last_px=report.last_px,
                                quote_currency=instrument.quote_currency,
                                commission=report.commission,
                                liquidity_side=report.liquidity_side,
                                ts_event=report.ts_event,
                            )
            elif order_status["status"] == "REJECTED":
                self.generate_order_rejected(
                    strategy_id=order.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=order_status["statusDescription"],
                    ts_event=self._clock.timestamp_ns(),
                )

    def _get_order_action(self, order: Order) -> EquityInstruction:
        positions = self._cache.positions(
            instrument_id=order.instrument_id,
        )

        current_position = None
        for p in positions:
            if not p.is_closed:
                current_position = p
                break

        is_long = current_position is not None and current_position.side == PositionSide.LONG
        is_short = current_position is not None and current_position.side == PositionSide.SHORT

        if order.side == OrderSide.SELL:
            if is_long:
                return EquityInstruction.SELL
            else:
                return EquityInstruction.SELL_SHORT

        if order.side == OrderSide.BUY:
            if is_short:
                return EquityInstruction.BUY_TO_COVER
            else:
                return EquityInstruction.BUY

        return EquityInstruction.SELL if order.side == OrderSide.SELL else EquityInstruction.BUY

    def _is_option_order(self, order: Order) -> bool:
        instrument = self._cache.instrument(order.instrument_id)

        if instrument is None:
            instrument = self._instrument_provider.find(order.instrument_id)

        if instrument is not None:
            return instrument.instrument_class == InstrumentClass.OPTION

        with suppress(ValueError):
            parse_opra_symbol(order.instrument_id.symbol.value)
            return True

        return False

    def _get_option_action(self, order: Order) -> OptionInstruction:
        positions = self._cache.positions(
            instrument_id=order.instrument_id,
        )

        current_position = None
        for p in positions:
            if not p.is_closed:
                current_position = p
                break

        is_long = current_position is not None and current_position.side == PositionSide.LONG
        is_short = current_position is not None and current_position.side == PositionSide.SHORT

        if order.side == OrderSide.SELL and is_long:
            return OptionInstruction.SELL_TO_CLOSE
        if order.side == OrderSide.BUY and is_short:
            return OptionInstruction.BUY_TO_CLOSE

        config_value = (
            self._config.default_instruction_option_sell
            if order.side == OrderSide.SELL
            else self._config.default_instruction_option_buy
        )
        try:
            return OptionInstruction(config_value)
        except ValueError:
            return (
                OptionInstruction.SELL_TO_CLOSE
                if order.side == OrderSide.SELL
                else OptionInstruction.BUY_TO_OPEN
            )

    def _add_order_leg(self, builder: OrderBuilder, order: Order) -> OrderBuilder:
        quantity = int(order.quantity)
        symbol = order.instrument_id.symbol.value

        if self._is_option_order(order):
            return builder.add_option_leg(
                self._get_option_action(order),
                symbol,
                quantity,
            )

        return builder.add_equity_leg(
            self._get_order_action(order),
            symbol,
            quantity,
        )

    async def _submit_limit_order(self, order: LimitOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_price(str(order.price))
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_market_order(self, order: MarketOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_stop_market_order(self, order: StopMarketOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_stop_price_link_basis(TRIGGER_TYPE_MAP[order.trigger_type])
            .set_stop_price(str(order.trigger_price))
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_stop_limit_order(self, order: StopMarketOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_price(str(order.price))
            .set_stop_price_link_basis(TRIGGER_TYPE_MAP[order.trigger_type])
            .set_stop_price(str(order.trigger_price))
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_trailing_stop_market_order(self, order: TrailingStopMarketOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_activation_price(str(order.trigger_price))
            .set_stop_price_link_basis(TRIGGER_TYPE_MAP[order.trigger_type])
            .set_stop_price_link_type(TRAILING_OFFSET_TYPE_MAP[order.trailing_offset_type])
            .set_stop_price_offset(float(order.trailing_offset))
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_trailing_stop_limit_order(self, order: TrailingStopLimitOrder) -> None:
        builder = (
            OrderBuilder()
            .set_order_type(ORDER_TYPE_MAP[order.order_type])
            .set_session(SchwabSession.NORMAL)
            .set_duration(TIME_IN_FORCE_MAP[order.time_in_force])
            .set_activation_price(str(order.trigger_price))
            .set_price(str(order.price))
            .set_stop_price_link_basis(TRIGGER_TYPE_MAP[order.trigger_type])
            .set_stop_price_link_type(TRAILING_OFFSET_TYPE_MAP[order.trailing_offset_type])
            .set_stop_price_offset(float(order.trailing_offset))
            .set_order_strategy_type(OrderStrategyType.SINGLE)
        )
        schwab_order = self._add_order_leg(builder, order).build()
        await self._submit_and_check_order(order, schwab_order)

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        self._log.warning("Order list submission not supported for Schwab")

    async def _modify_order(self, command: ModifyOrder) -> None:
        self._log.warning(
            "Order modification not supported for Schwab, the replace_order endpoint will cancel the old order and create a new one",
        )

    async def _cancel_order(self, command: CancelOrder) -> None:
        order: Order | None = self._cache.order(command.client_order_id)

        if order is None:
            self._log.error(f"{command.client_order_id!r} not found in cache")
            return

        if order.is_closed:
            self._log.warning(
                f"`CancelOrder` command for {command.client_order_id!r} when order already {
                    order.status_string()
                } (will not send to exchange)",
            )
            return

        client_order_id = command.client_order_id.value
        venue_order_id = (
            str(
                command.venue_order_id,
            )
            if command.venue_order_id
            else None
        )

        if venue_order_id is None:
            self._log.error(
                f"Unable to cancel {command.client_order_id}: missing venue order id",
            )
            return

        retry_manager = await self._retry_manager_pool.acquire()
        try:
            response = await retry_manager.run(
                "cancel_order",
                [client_order_id, venue_order_id],
                self._http_client.cancel_order,
                order_id=venue_order_id,
                account_hash=self._account_hash,
            )

            if not retry_manager.result:
                self.generate_order_cancel_rejected(
                    order.strategy_id,
                    order.instrument_id,
                    order.client_order_id,
                    order.venue_order_id,
                    retry_manager.message,
                    self._clock.timestamp_ns(),
                )

            if response:
                ret_code = response.status_code

                if ret_code != 0:
                    if ret_code == 200:
                        self.generate_order_canceled(
                            strategy_id=order.strategy_id,
                            instrument_id=order.instrument_id,
                            client_order_id=order.client_order_id,
                            venue_order_id=order.venue_order_id,
                            ts_event=self._clock.timestamp_ns(),
                        )
                    else:
                        self.generate_order_cancel_rejected(
                            strategy_id=order.strategy_id,
                            instrument_id=order.instrument_id,
                            client_order_id=order.client_order_id,
                            venue_order_id=order.venue_order_id,
                            reason=response.json()["message"],
                            ts_event=self._clock.timestamp_ns(),
                        )
        finally:
            await self._retry_manager_pool.release(retry_manager)

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        cancels = command.cancels if hasattr(command, "cancels") else []
        for cancel in cancels:
            await self._cancel_order(cancel)

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        open_orders: list[Order] = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
        )

        # TODO: A future improvement could be to asyncio.gather all cancel tasks
        for order in open_orders:
            retry_manager = await self._retry_manager_pool.acquire()
            try:
                response = await retry_manager.run(
                    "cancel_order",
                    [order.client_order_id, order.venue_order_id],
                    self._http_client.cancel_order,
                    order_id=order.venue_order_id,
                    account_hash=self._account_hash,
                )

                if not retry_manager.result:
                    self.generate_order_cancel_rejected(
                        order.strategy_id,
                        order.instrument_id,
                        order.client_order_id,
                        order.venue_order_id,
                        retry_manager.message,
                        self._clock.timestamp_ns(),
                    )
                if response:
                    ret_code = response.status_code

                    if ret_code != 0:
                        if ret_code == 200:
                            self.generate_order_canceled(
                                strategy_id=order.strategy_id,
                                instrument_id=order.instrument_id,
                                client_order_id=order.client_order_id,
                                venue_order_id=order.venue_order_id,
                                ts_event=self._clock.timestamp_ns(),
                            )
                        else:
                            self.generate_order_cancel_rejected(
                                strategy_id=order.strategy_id,
                                instrument_id=order.instrument_id,
                                client_order_id=order.client_order_id,
                                venue_order_id=order.venue_order_id,
                                reason=response.json()["message"],
                                ts_event=self._clock.timestamp_ns(),
                            )
            finally:
                await self._retry_manager_pool.release(retry_manager)

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        instrument_id = command.instrument_id
        client_order_id = command.client_order_id
        venue_order_id = command.venue_order_id

        if venue_order_id is None and client_order_id is not None:
            venue_order_id = self._client_order_to_venue.get(client_order_id)

        if venue_order_id:
            order_data = await self._http_client.get_order(venue_order_id, self._account_hash)
        else:
            return None

        report = self._build_order_status_report(
            order_data,
            instrument_id,
        )
        return report

    async def generate_order_status_reports(  # noqa: C901
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        try:
            orders = await self._http_client.get_orders_for_account(
                account_hash=self._account_hash,
                from_entered_datetime=command.start,
                to_entered_datetime=command.end,
            )
        except Exception as exc:
            self._log.exception("Failed to list orders", exc)
            return []

        reports: list[OrderStatusReport] = []
        start_utc = self._normalize_datetime(command.start)
        end_utc = self._normalize_datetime(command.end)

        for order_data in orders:
            raw_order_id = order_data.get("orderId")
            venue_order_id: VenueOrderId | None = None

            if raw_order_id:
                try:
                    venue_order_id = VenueOrderId(str(raw_order_id))
                except Exception:
                    venue_order_id = None

            client_order_id = (
                self._cache.client_order_id(venue_order_id) if venue_order_id else None
            )

            instrument_id = self._infer_instrument_id(order_data)

            if instrument_id is None and client_order_id is not None:
                instrument_id = self._client_order_to_instrument.get(client_order_id)

            if command.instrument_id and instrument_id != command.instrument_id:
                continue

            entered_time_str = order_data.get("enteredTime")

            if entered_time_str and (start_utc or end_utc):
                try:
                    entered_time = pd.to_datetime(entered_time_str, utc=True).to_pydatetime()
                except (TypeError, ValueError) as exc:
                    self._log.warning(
                        f"Failed to parse enteredTime for order {raw_order_id}: {entered_time_str}",
                        exc_info=exc,
                    )
                else:
                    if start_utc and entered_time < start_utc:
                        continue
                    if end_utc and entered_time > end_utc:
                        continue

            status = SCHWAB_STATUS_MAP.get(
                str(order_data.get("status", "")).upper(),
                OrderStatus.ACCEPTED,
            )

            if command.open_only and status not in (OrderStatus.SUBMITTED, OrderStatus.ACCEPTED):
                continue

            if instrument_id is None:
                self._log.debug(
                    f"Skipping order {raw_order_id}: unable to resolve instrument",
                )
                continue

            try:
                report = self._build_order_status_report(order_data, instrument_id)
            except Exception as exc:
                self._log.exception(
                    f"Failed to build OrderStatusReport for order {raw_order_id}",
                    exc,
                )
                continue

            if report is not None:
                reports.append(report)
        return reports

    async def generate_fill_reports(  # noqa: C901
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        try:
            orders = await self._http_client.get_orders_for_account(
                account_hash=self._account_hash,
                from_entered_datetime=command.start,
                to_entered_datetime=command.end,
            )
        except Exception as exc:
            self._log.exception("Failed to list orders for fill reports", exc)
            return []

        reports: list[FillReport] = []
        start_utc = self._normalize_datetime(command.start)
        end_utc = self._normalize_datetime(command.end)
        venue_order_filter = str(command.venue_order_id) if command.venue_order_id else None

        instruments = []
        for order_data in orders:
            instrument_id = self._infer_instrument_id(order_data)
            instruments.append(instrument_id)
        await self._ensure_instruments_loaded(instruments)

        for order_data in orders:
            raw_order_id = order_data.get("orderId")

            if venue_order_filter and str(raw_order_id) != venue_order_filter:
                continue

            instrument_id = self._infer_instrument_id(order_data)
            venue_order_id: VenueOrderId | None = None

            if raw_order_id:
                try:
                    venue_order_id = VenueOrderId(str(raw_order_id))
                except Exception:
                    venue_order_id = None

            if instrument_id is None and venue_order_id is not None:
                client_order_id = self._cache.client_order_id(venue_order_id)

                if client_order_id in self._client_order_to_instrument:
                    instrument_id = self._client_order_to_instrument[client_order_id]

            if command.instrument_id and instrument_id != command.instrument_id:
                continue

            if instrument_id is None or venue_order_id is None:
                self._log.debug(
                    f"Skipping fills for order {raw_order_id}: missing instrument or venue order id",
                )
                continue

            instrument = await self._ensure_instrument_loaded(instrument_id)

            if instrument is None:
                self._log.warning(
                    f"Unable to load instrument {instrument_id} for fill report generation",
                )
                continue

            client_order_id = self._cache.client_order_id(venue_order_id)
            order_side = self._parse_order_side(order_data)
            fills = self._extract_fills(order_data)

            if not fills:
                continue

            for fill in fills:
                fill_time = fill.get("time") or fill.get("executionTime")

                if fill_time and (start_utc or end_utc):
                    try:
                        fill_dt = pd.to_datetime(fill_time, utc=True).to_pydatetime()
                    except (TypeError, ValueError) as exc:
                        self._log.warning(
                            f"Failed to parse fill time for order {raw_order_id}: {fill_time}",
                            exc_info=exc,
                        )
                        fill_dt = None
                    if fill_dt:
                        if start_utc and fill_dt < start_utc:
                            continue
                        if end_utc and fill_dt > end_utc:
                            continue

                report = self._build_fill_report(
                    fill,
                    instrument=instrument,
                    instrument_id=instrument_id,
                    venue_order_id=venue_order_id,
                    client_order_id=client_order_id,
                    order_side=order_side,
                )

                if report is not None:
                    reports.append(report)

        return reports

    async def generate_position_status_reports(  # noqa: C901
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        try:
            positions = await self._http_client.get_positions(self._account_hash)
        except Exception as exc:
            self._log.exception("Failed to retrieve positions", exc)
            return []

        reports: list[PositionStatusReport] = []
        ts_now = self._clock.timestamp_ns()

        if not positions:
            if command.instrument_id is not None:
                instrument = await self._ensure_instrument_loaded(command.instrument_id)

                if instrument is not None:
                    reports.append(
                        PositionStatusReport.create_flat(
                            account_id=self.account_id,
                            instrument_id=instrument.id,
                            size_precision=instrument.size_precision,
                            ts_init=ts_now,
                        ),
                    )
            return reports

        instruments = []
        for position in positions:
            instrument_info = position.get("instrument")
            if not isinstance(instrument_info, Mapping):
                continue
            symbol = instrument_info.get("symbol")
            asset_type = instrument_info.get("assetType", "EQUITY")
            if not symbol:
                continue
            instrument_id = self._instrument_id_from_symbol(str(symbol), str(asset_type))
            instruments.append(instrument_id)
        await self._ensure_instruments_loaded(instruments)

        for position in positions:
            report = await self._build_position_report(position, ts_now, command.instrument_id)

            if report is not None:
                reports.append(report)

        if not reports and command.instrument_id is not None:
            instrument = await self._ensure_instrument_loaded(command.instrument_id)

            if instrument is not None:
                reports.append(
                    PositionStatusReport.create_flat(
                        account_id=self.account_id,
                        instrument_id=instrument.id,
                        size_precision=instrument.size_precision,
                        ts_init=ts_now,
                    ),
                )

        return reports

    # -- Helpers ---------------------------------------------------------------------------------

    @staticmethod
    def _normalize_datetime(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    async def _ensure_instruments_loaded(self, instrument_ids: list[InstrumentId]) -> None:
        new_instruments = []
        for instrument_id in instrument_ids:
            if not self._cache.instrument(instrument_id):
                new_instruments.append(instrument_id)
        if not new_instruments:
            return
        try:
            await self._instrument_provider.load_ids_async(new_instruments)
        except Exception as exc:
            self._log.warning(f"Unable to load instrument {instrument_id}", exc_info=exc)
            return

        for instrument_id in new_instruments:
            if not self._cache.instrument(instrument_id):
                instrument = self._instrument_provider.find(instrument_id)
                if instrument:
                    self._cache.add_instrument(instrument)
                else:
                    self._log.warning(f"Unable to find instrument {instrument_id}")

    async def _ensure_instrument_loaded(
        self,
        instrument_id: InstrumentId | None,
    ) -> Instrument | None:
        if instrument_id is None:
            return None
        instrument = self._cache.instrument(instrument_id)

        if instrument is not None:
            return instrument
        try:
            await self._instrument_provider.load_async(instrument_id)
        except Exception as exc:
            self._log.warning(f"Unable to load instrument {instrument_id}", exc_info=exc)
            return None
        if not self._cache.instrument(instrument_id):
            instrument = self._instrument_provider.find(instrument_id)
            if instrument:
                self._cache.add_instrument(instrument)
            else:
                self._log.warning(f"Unable to find instrument {instrument_id}")
        return self._cache.instrument(instrument_id)

    def _build_fill_report(
        self,
        fill: Mapping[str, Any],
        *,
        instrument: Instrument,
        instrument_id: InstrumentId,
        venue_order_id: VenueOrderId,
        client_order_id: ClientOrderId | None,
        order_side: OrderSide,
    ) -> FillReport | None:
        quantity_value = fill.get("quantity") or fill.get("fillQuantity") or fill.get("legQuantity")

        if quantity_value is None:
            return None

        try:
            qty = float(quantity_value)
        except (TypeError, ValueError):
            return None

        if qty <= 0:
            return None

        price_value = fill.get("price") or fill.get("executionPrice")
        try:
            price = float(price_value) if price_value is not None else 0.0
        except (TypeError, ValueError):
            price = 0.0

        ts_event = self._clock.timestamp_ns()
        fill_time = fill.get("time") or fill.get("executionTime")

        if fill_time:
            with suppress(TypeError, ValueError):
                ts_event = int(pd.to_datetime(fill_time, utc=True).value)

        execution_id = fill.get("executionId")
        leg_id = fill.get("legId")

        if execution_id and leg_id:
            raw_trade_id = f"{execution_id}-{leg_id}"
        elif execution_id:
            raw_trade_id = str(execution_id)
        elif leg_id:
            raw_trade_id = f"{venue_order_id}-{leg_id}"
        else:
            raw_trade_id = UUID4().value

        trade_id = TradeId(str(raw_trade_id))

        currency = getattr(instrument, "currency", USD)
        commission_raw = fill.get("commission")
        try:
            commission_value = float(commission_raw) if commission_raw is not None else 0.0
        except (TypeError, ValueError):
            commission_value = 0.0
        commission = Money(commission_value, currency)

        last_qty = instrument.make_qty(abs(qty))
        last_px = instrument.make_price(price)

        return FillReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            venue_order_id=venue_order_id,
            trade_id=trade_id,
            order_side=order_side,
            last_qty=last_qty,
            last_px=last_px,
            commission=commission,
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=ts_event,
            ts_init=self._clock.timestamp_ns(),
            client_order_id=client_order_id,
            venue_position_id=None,
        )

    # -- WebSocket handling ----------------------------------------------------------------------

    def _handle_ws_message(self, raw: bytes) -> None:
        try:
            msg = msgspec_json.decode(raw)
        except Exception as exc:
            self._log.exception("Failed to decode Schwab websocket payload", exc)
            return

        for section in ("notify", "data"):
            entries = msg.get(section)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                if entry.get("service") != "ACCT_ACTIVITY":
                    continue
                if "heartbeat" in entry:
                    continue
                self._handle_account_activity_message(entry)

    def _handle_account_activity_message(self, msg: Mapping[str, Any]) -> None:
        labeled_msg = self._label_account_activity_message(msg)
        content = labeled_msg.get("content")
        if not isinstance(content, list):
            return

        for entry in content:
            if not isinstance(entry, Mapping):
                continue
            payload = entry.get("MESSAGE_DATA")
            if not payload:
                continue
            order_payload = self._parse_account_activity_payload(payload)
            if order_payload is None:
                continue
            self._handle_order_activity_payload(order_payload)

    def _label_account_activity_message(self, msg: Mapping[str, Any]) -> Mapping[str, Any]:
        if "content" not in msg:
            return msg
        labeled = copy.deepcopy(msg)
        content = msg.get("content")
        labeled_content = labeled.get("content")
        if not isinstance(content, list) or not isinstance(labeled_content, list):
            return msg
        for idx, entry in enumerate(content):
            if not isinstance(entry, Mapping):
                continue
            try:
                StreamClient.AccountActivityFields.relabel_message(entry, labeled_content[idx])
            except Exception as exc:
                self._log.debug(
                    f"Unable to relabel account activity entry {idx}: {exc}",
                )
        return labeled

    def _parse_account_activity_payload(self, payload: Any) -> Mapping[str, Any] | None:
        if isinstance(payload, Mapping):
            return payload
        if not isinstance(payload, str):
            return None
        data = payload.strip()
        if not data:
            return None
        try:
            parsed = json.loads(data)
        except (TypeError, ValueError):
            self._log.debug(f"Unable to parse account activity payload: {data}")
            return None
        if isinstance(parsed, list):
            parsed = parsed[0] if parsed else None
        if isinstance(parsed, Mapping):
            return parsed
        return None

    def _handle_order_activity_payload(  # noqa: C901
        self,
        order_data: Mapping[str, Any],
    ) -> None:
        raw_order_id = order_data.get("orderId") or order_data.get("order_id")
        if raw_order_id is None:
            return

        venue_order_id: VenueOrderId | None = None
        try:
            venue_order_id = VenueOrderId(str(raw_order_id))
        except Exception:
            self._log.debug(f"Unable to parse venue order id from {raw_order_id}")
            return

        client_order_id = self._cache.client_order_id(venue_order_id)
        if client_order_id is None:
            client_order_id = self._venue_order_to_client.get(venue_order_id)

        if client_order_id is None:
            self._log.debug(
                f"Skipping Schwab account activity for unknown venue order {venue_order_id}",
            )
            return

        order = self._cache.order(client_order_id)
        if order is None:
            self._log.debug(
                f"Skipping account activity: order {client_order_id} missing from cache"
            )
            return

        instrument_id = self._infer_instrument_id(order_data) or order.instrument_id
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.debug(f"Skipping account activity: instrument {instrument_id} not cached")
            return

        fills = self._extract_fills(order_data)
        if not fills:
            return

        for fill in fills:
            report = self._build_fill_report(
                fill,
                instrument=instrument,
                instrument_id=instrument.id,
                venue_order_id=venue_order_id,
                client_order_id=client_order_id,
                order_side=order.side,
            )
            if report is None:
                continue
            if self._has_seen_stream_trade_id(report.trade_id):
                continue
            self._remember_stream_trade_id(report.trade_id)
            self.generate_order_filled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=report.trade_id,
                order_side=report.order_side,
                order_type=order.order_type,
                last_qty=report.last_qty,
                last_px=report.last_px,
                quote_currency=instrument.quote_currency,
                commission=report.commission,
                liquidity_side=report.liquidity_side,
                ts_event=report.ts_event,
            )

    def _remember_stream_trade_id(self, trade_id: TradeId) -> None:
        trade_value = trade_id.value
        self._recent_stream_trade_ids.append(trade_value)
        self._recent_stream_trade_ids_set.add(trade_value)
        while len(self._recent_stream_trade_ids) > self._recent_stream_trade_ids_limit:
            oldest = self._recent_stream_trade_ids.popleft()
            self._recent_stream_trade_ids_set.discard(oldest)

    def _has_seen_stream_trade_id(self, trade_id: TradeId) -> bool:
        return trade_id.value in self._recent_stream_trade_ids_set

    async def _build_position_report(  # noqa: C901
        self,
        position: Mapping[str, Any],
        ts: int,
        requested_instrument: InstrumentId | None,
    ) -> PositionStatusReport | None:
        instrument_info = position.get("instrument")

        if not isinstance(instrument_info, Mapping):
            return None

        symbol = instrument_info.get("symbol")
        asset_type = instrument_info.get("assetType", "EQUITY")

        if not symbol:
            return None

        instrument_id = self._instrument_id_from_symbol(str(symbol), str(asset_type))

        if requested_instrument and instrument_id != requested_instrument:
            return None

        instrument = await self._ensure_instrument_loaded(instrument_id)

        if instrument is None:
            return None

        long_qty = position.get("longQuantity", 0.0)
        short_qty = position.get("shortQuantity", 0.0)
        try:
            net_qty = float(long_qty) - float(short_qty)
        except (TypeError, ValueError):
            return None

        if net_qty > 0:
            side = PositionSide.LONG
            qty = instrument.make_qty(net_qty)
        elif net_qty < 0:
            side = PositionSide.SHORT
            qty = instrument.make_qty(abs(net_qty))
        else:
            if requested_instrument and instrument_id == requested_instrument:
                side = PositionSide.FLAT
                qty = instrument.make_qty(0.0)
            else:
                return None

        avg_price_value = position.get("averagePrice") or position.get("averagePriceForShort")
        avg_px_open = None

        if avg_price_value is not None:
            try:
                avg_px_open = instrument.make_price(float(avg_price_value)).as_decimal()
            except (TypeError, ValueError):
                avg_px_open = None

        return PositionStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            position_side=side,
            quantity=qty,
            report_id=UUID4(),
            ts_last=ts,
            ts_init=ts,
            avg_px_open=avg_px_open,
        )

    def _build_order_status_report(
        self,
        order_data: Mapping[str, Any],
        instrument_id: InstrumentId,
    ) -> OrderStatusReport | None:
        venue_order_id = VenueOrderId(str(order_data.get("orderId", "")))
        client_order_id = self._cache.client_order_id(venue_order_id)
        status = SCHWAB_STATUS_MAP.get(
            order_data.get(
                "status",
                "",
            ).upper(),
            OrderStatus.ACCEPTED,
        )

        filled_qty = float(order_data.get("filledQuantity", 0.0))
        total_qty = float(order_data.get("quantity", 0.0))

        # Treat fully filled CANCELED orders as FILLED
        if status == OrderStatus.CANCELED and filled_qty > 0 and filled_qty >= total_qty:
            status = OrderStatus.FILLED

        # Handle partial fills
        if status == OrderStatus.SUBMITTED and 0 < filled_qty < total_qty:
            status = OrderStatus.PARTIALLY_FILLED

        raw_type = str(order_data.get("orderType", "")).upper()
        order_type_enum = ORDER_TYPE_REVERSE.get(SchwabOrderType(raw_type))
        tif_value = str(
            order_data.get(
                "duration",
                self._config.default_duration,
            ),
        )
        time_in_force = TIME_IN_FORCE_REVERSE.get(
            SchwabDuration(tif_value),
            TimeInForce.DAY,
        )

        if order_type_enum in (OrderType.TRAILING_STOP_MARKET, OrderType.TRAILING_STOP_LIMIT):
            # TODO: need to refine here
            self._log.warning("Trailing stop orders not supported for now!")
            return None
        stop_price = order_data.get("stopPrice", None)

        if stop_price:
            stop_price = Price.from_str(str(stop_price))
        stop_type = order_data.get("stopType", None)

        if stop_type == "STANDARD":
            stop_type = TriggerType.DEFAULT
        elif stop_type == "LAST":
            stop_type = TriggerType.LAST_PRICE
        else:
            stop_type = TriggerType.NO_TRIGGER

        limit_price = order_data.get("price")

        if limit_price:
            limit_price = Price.from_str(str(limit_price))

        avg_price = Decimal(self._parse_avg_price(order_data))

        ts_init = self._clock.timestamp_ns()

        ts_accepted = ts_init
        entered_time_str = order_data.get("enteredTime")

        if entered_time_str:
            ts_accepted = int(
                pd.to_datetime(
                    entered_time_str,
                ).timestamp()
                * 1e9,
            )

        ts_last = ts_init
        close_time_str = order_data.get("closeTime")

        if close_time_str:
            ts_last = int(pd.to_datetime(close_time_str).timestamp() * 1e9)

        report = OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument_id,
            venue_order_id=venue_order_id,
            client_order_id=client_order_id,
            order_side=self._parse_order_side(order_data),
            order_type=order_type_enum,
            time_in_force=time_in_force,
            order_status=status,
            quantity=Quantity.from_str(str(total_qty)),
            filled_qty=Quantity.from_str(str(filled_qty)),
            avg_px=avg_price,
            report_id=UUID4(),
            ts_accepted=ts_accepted,
            ts_last=ts_last,
            ts_init=ts_init,
            price=limit_price,
            trigger_price=stop_price,
            trigger_type=stop_type,
        )
        return report

    def _parse_avg_price(self, order_data: Mapping[str, Any]) -> float:
        total_value = 0.0
        total_qty = 0.0

        activities = order_data.get("orderActivityCollection")
        if isinstance(activities, list):
            for activity in activities:
                if activity.get("activityType") != "EXECUTION":
                    continue

                # Skip cancellations/rejections if they appear in activity collection
                if activity.get("executionType") in ("CANCELED", "REJECTED", "EXPIRED"):
                    continue

                legs = activity.get("executionLegs")
                if isinstance(legs, list):
                    for leg in legs:
                        qty = float(leg.get("quantity", 0.0))
                        price = float(leg.get("price", 0.0))
                        if qty > 0:
                            total_value += price * qty
                            total_qty += qty

        return total_value / total_qty if total_qty > 0 else 0.0

    def _parse_order_side(self, order_data: Mapping[str, Any]) -> OrderSide:
        legs = order_data.get("orderLegCollection")

        if isinstance(legs, list) and legs:
            instruction = str(legs[0].get("instruction", "")).upper()

            if "SELL" in instruction:
                return OrderSide.SELL
        return OrderSide.BUY

    def _extract_fills(self, order_data: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        activities = order_data.get("orderActivityCollection", [])
        fills: list[Mapping[str, Any]] = []

        if isinstance(activities, list):
            for activity in activities:
                activity_type = activity.get("activityType")
                if activity_type != "EXECUTION":
                    self._log.debug(f"Skipping order activity type: {activity_type}")
                    continue

                execution_type = activity.get("executionType")
                if execution_type in ("CANCELED", "REJECTED", "EXPIRED"):
                    continue

                execution_id = activity.get("executionId")
                execution_legs = (
                    activity.get("executionLegs")
                    if isinstance(
                        activity,
                        Mapping,
                    )
                    else None
                )

                if isinstance(execution_legs, list):
                    for leg in execution_legs:
                        if isinstance(leg, Mapping):
                            leg_data = dict(leg)
                            if execution_id:
                                leg_data["executionId"] = execution_id
                            fills.append(leg_data)
        return fills

    def _instrument_id_from_symbol(self, symbol: str, asset_type: str) -> InstrumentId:
        venue = None

        if asset_type.upper() == "OPTION":
            # option_exchange = SCHWAB_OPTION_VENUE.value
            option_exchange = SCHWAB_VENUE.value
            # provider_config = getattr(
            #     self._config.instrument_provider,
            #     "option_exchange",
            #     None,
            # )
            # if isinstance(provider_config, str) and provider_config:
            #     option_exchange = provider_config
            venue = option_exchange
        else:
            # TODO: should not be hardcoded
            venue = self.venue.value
        return InstrumentId.from_str(f"{symbol}.{venue}")

    def _infer_instrument_id(self, order_data: Mapping[str, Any]) -> InstrumentId | None:
        legs = order_data.get("orderLegCollection")

        if not isinstance(legs, list) or not legs:
            return None
        instrument_payload = legs[0].get("instrument")

        if not isinstance(instrument_payload, Mapping):
            return None
        symbol = instrument_payload.get("symbol")
        asset_type = instrument_payload.get("assetType", "EQUITY")

        if not symbol:
            return None
        return self._instrument_id_from_symbol(symbol, asset_type)

    # def _coerce_time_number(self, value: float) -> int:
    #     value_int = int(value)
    #     if value_int > 10**18:
    #         return value_int
    #     if value_int > 10**12:
    #         return value_int * 1_000_000
    #     if value_int > 10**9:
    #         return value_int * 1_000
    #     return value_int * 1_000_000_000


__all__ = ["SchwabExecutionClient"]
