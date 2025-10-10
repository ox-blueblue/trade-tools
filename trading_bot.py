"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.position_amt = 0
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None
        self.grid_except_price = None 

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time
        # has placed new orders
        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0
        # reach max orders
        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1
        # dynamic cool down time based on active close orders
        # if len(self.active_close_orders) / self.config.max_orders >= 2/3:
        #     cool_down_time = 2 * self.config.wait_time
        # elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
        #     cool_down_time = self.config.wait_time
        # elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
        #     cool_down_time = self.config.wait_time / 2
        # else:
        #     cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, not set cooldown_time
        if self.last_open_order_time == 0:
            self.last_open_order_time = time.time()
            return 0

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return int(cool_down_time - (time.time() - self.last_open_order_time))

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False
            order_id = order_result.order_id
            self.logger.log(f"[{self.current_order_status}] [{order_id}] {order_result.status} "
                            f"{self.config.quantity} @ {order_result.price}", "INFO")

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price
        try:
            if self.order_filled_event.is_set() or order_result.status == 'FILLED':
                if self.config.boost_mode:
                    close_order_result = await self.exchange_client.place_market_order(
                        self.config.contract_id,
                        self.config.quantity,
                        self.config.close_order_side
                    )
                    self.logger.log(f"[CLOSE] [{close_order_result.order_id}] New "
                        f"{self.config.quantity} @ market", "INFO")                        
                else:
                    self.last_open_order_time = time.time()
                    # Place close order
                    close_side = self.config.close_order_side
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self.exchange_client.place_close_order(
                        self.config.contract_id,
                        self.config.quantity,
                        close_price,
                        close_side
                    )

                    await asyncio.sleep(1)

                    if not close_order_result.success:
                        self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                        raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")
                    
                    self.logger.log(f"[CLOSE] [{close_order_result.order_id}] {close_order_result.status} "
                            f"{self.config.quantity} @ {close_price}", "INFO")            
            else:
                self.order_canceled_event.clear()
                # Cancel the order if it's still open
                self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "ERROR")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack":
                    self.order_filled_amount = cancel_result.filled_size
                    self.logger.log(f"[CALCEL] backpack cancel order filled amount: {self.order_filled_amount}", "DEBUG")
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size

                if self.order_filled_amount > 0:
                    close_side = self.config.close_order_side
                    if self.config.boost_mode:
                        close_order_result = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            self.order_filled_amount,
                            close_side
                        )
                    else:
                        if close_side == 'sell':
                            close_price = filled_price * (1 + self.config.take_profit/100)
                        else:
                            close_price = filled_price * (1 - self.config.take_profit/100)

                        close_order_result = await self.exchange_client.place_close_order(
                            self.config.contract_id,
                            self.order_filled_amount,
                            close_price,
                            close_side
                        )
                        if self.config.exchange == "lighter":
                            await asyncio.sleep(1)

                    self.logger.log(f"[CLOSE] [{close_order_result.order_id}] {close_order_result.status} "
                            f"{self.order_filled_amount} @ {close_price}", "INFO")

                    self.last_open_order_time = time.time()

                    if not close_order_result.success:
                        self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
            return True
        except Exception as e:
            self.logger.log(f"[CLOSE] Failed to handle close order: {e}", "ERROR")       
            return False             


    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:   
                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {self.position_amt} | Active closing orders/amount: {len(self.active_close_orders)}/{active_close_amount} | "
                                f"Gird except price: {self.grid_except_price}")
                self.last_log_time = time.time()
                              

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")
        

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            # 多订单的时候选择第二个作为基准，避免订单密集时，步长失效
            step_count = 1
            # sort active close orders by price by direction
            if len(self.active_close_orders) > 1:
                self.active_close_orders = sorted(self.active_close_orders, key=lambda o: o["price"], reverse=self.config.direction == "sell")
                next_close_order = self.active_close_orders[1]
                next_close_price = next_close_order["price"]
                step_count = 2
            elif len(self.active_close_orders) == 1:            
                next_close_order = self.active_close_orders[0]
                next_close_price = next_close_order["price"]
            self.logger.log(f"step_count: {step_count}, next_close_price: {next_close_price}", "DEBUG")
            try:
                best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
                if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                    self.logger.log(f"No bid/ask data available", "ERROR")
                    return False
            except Exception as e:
                self.logger.log(f"Exception:No bid/ask data fetched", "ERROR")
                return False

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100*step_count:
                    return True
                else:
                    self.grid_except_price = round(next_close_price / 
                                                   (1 + self.config.grid_step/100*step_count) / (1 - self.config.take_profit/100), 2)
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100*step_count:
                    return True
                else:
                    self.grid_except_price = round(next_close_price / 
                                                   (1 + self.config.grid_step/100*step_count) / (1 - self.config.take_profit/100), 2)
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True
        

    async def _rebalance_position(self):
        try:
            active_close_amount = sum(
                Decimal(order.get('size', 0))
                for order in self.active_close_orders
                if isinstance(order, dict)
            )

            if self.position_amt > active_close_amount:                
                # Close extra positions
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
                self.logger.log(f"[CLOSE] [{close_order_result.order_id}] New "
                    f"{self.config.quantity} @ market", "INFO")  
            elif self.position_amt < active_close_amount:
                self.logger.log("Position less than active closing amount", "ERROR")
            return True
        except Exception as e:
            self.logger.log(f"[CLOSE] Failed to rebalance position: {e}", "ERROR")
            return False
                 

    async def _check_price_condition(self) -> bool:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        try:
            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                self.logger.log(f"No bid/ask data available", "ERROR")
                return stop_trading, pause_trading
        except Exception as e:
            self.logger.log(f"Exception:No bid/ask data fetched", "ERROR")
            return stop_trading, pause_trading

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    async def _lark_bot_notify(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as bot:
                await bot.send_text(message)
    
    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            # Main trading loop
            # wait should be set to 1 second because for judge new market and order
            mismatch_detected_count = 0
            while not self.shutdown_requested:      
                # Update active orders
                active_close_orders_filled = False
                try:
                    for _ in range(3):
                        active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)
                        if len(active_orders) > 0:
                            break
                        await asyncio.sleep(1)                       

                    # Get active close orders
                    active_close_orders_tmp = []
                    for order in active_orders:
                        if order.side == self.config.close_order_side:
                            active_close_orders_tmp.append({
                                'id': order.order_id,
                                'price': order.price,
                                'size': order.size
                            })
                    if len(active_close_orders_tmp) < len(self.active_close_orders):
                        active_close_orders_filled = True
                        self.logger.log(f"Active closing orders from {len(self.active_close_orders)} to {len(active_close_orders_tmp)}", "INFO")

                    self.active_close_orders = active_close_orders_tmp
                    # Calculate active closing amount
                    active_close_amount = sum(
                        Decimal(order.get('size', 0))
                        for order in self.active_close_orders
                        if isinstance(order, dict)
                    )
                    # Get current position
                    self.position_amt = await self.exchange_client.get_account_positions()                    
                except Exception as e:
                    self.logger.log(f"Error fetching active orders or positions: {e}", "ERROR")
                    await asyncio.sleep(1)
                    continue

                # Periodic logging
                await self._log_status_periodically()

                # Check for position mismatch
                if abs(self.position_amt - active_close_amount) > (1 * self.config.quantity):                    
                    mismatch_detected_count += 1
                else:
                    mismatch_detected_count = 0

                # Process mismatch
                if mismatch_detected_count >= 3:
                    mismatch_detected_count = 0
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"  
                    error_message += f"Current Position: {self.position_amt} | Active closing orders/amount: {len(self.active_close_orders)}/{active_close_amount}\n"                    
                    error_message += "Will auto rebalance position\n"
                    self.logger.log(error_message, "ERROR")
                    await self._lark_bot_notify(error_message.lstrip())
                    if not await self._rebalance_position():
                        await asyncio.sleep(1)                    
                        continue

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price\n"
                    await self.graceful_shutdown(msg)
                    await self._lark_bot_notify(msg.lstrip())
                    continue

                if pause_trading:
                    self.logger.log(f"Pause trading due to pause price", "INFO")
                    await asyncio.sleep(5)
                    continue
            
                # 订单有成交，则立即进行下一次交易
                if not active_close_orders_filled:
                    meet_grid_step_condition = await self._meet_grid_step_condition()
                    if not meet_grid_step_condition:
                        self.logger.log(f"Grid step condition not met", "DEBUG")
                        await asyncio.sleep(1)
                        continue

                    wait_time = self._calculate_wait_time()
                    if wait_time > 0:             
                        self.logger.log(f"Wait time condition not met, remain {wait_time} seconds", "DEBUG")       
                        await asyncio.sleep(1)
                        continue 
                # Condition met, place new open order
                await self._place_and_monitor_open_order()                   
                self.last_close_orders += 1                             

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
