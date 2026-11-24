import asyncio
import json
import math
import time
from datetime import datetime
import aiohttp
import websockets
import flet as ft

# =====================================================================
# 1. محرك المؤشرات الفنية اليدوي (Pure Python - خفيف وسريع للأندرويد)
# =====================================================================
class TechnicalIndicators:
    @staticmethod
    def calculate_ema(prices: list[float], period: int) -> list[float]:
        if len(prices) < period:
            return []
        multiplier = 2 / (period + 1)
        ema_values = [sum(prices[:period]) / period]
        for price in prices[period:]:
            ema_values.append((price - ema_values[-1]) * multiplier + ema_values[-1])
        return ema_values

    @staticmethod
    def calculate_rsi(prices: list[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0.0 for d in deltas[:period]]
        losses = [-d if d < 0 else 0.0 for d in deltas[:period]]

        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period

        for d in deltas[period:]:
            gain = d if d > 0 else 0.0
            loss = -d if d < 0 else 0.0
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return round(100.0 - (100.0 / (1.0 + rs)), 2)

    @staticmethod
    def calculate_bollinger_bands(prices: list[float], period: int = 20, std_dev: float = 2.0):
        if len(prices) < period:
            return None, None, None
        window = prices[-period:]
        sma = sum(window) / period
        variance = sum((p - sma) ** 2 for p in window) / period
        sd = math.sqrt(variance)
        upper = sma + (std_dev * sd)
        lower = sma - (std_dev * sd)
        return upper, sma, lower

    @staticmethod
    def find_support_resistance(candles: list[dict], lookback: int = 40, window: int = 3):
        if len(candles) < lookback:
            return [], []
        recent = candles[-lookback:]
        supports = []
        resistances = []
        for i in range(window, len(recent) - window):
            highs = [c["high"] for c in recent[i - window : i + window + 1]]
            lows = [c["low"] for c in recent[i - window : i + window + 1]]
            if recent[i]["high"] == max(highs):
                resistances.append(recent[i]["high"])
            if recent[i]["low"] == min(lows):
                supports.append(recent[i]["low"])
        return supports, resistances


# =====================================================================
# 2. محرك التداول والاتصال بمنصة Deriv (WebSocket API v3)
# =====================================================================
class DerivTradingEngine:
    def __init__(self, ui_callback):
        self.ui_callback = ui_callback
        self.ws_url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
        self.ws = None
        self.is_running = False
        self.is_connected = False

        # إعدادات المستخدم الافتراضية
        self.config = {
            "api_token": "",
            "symbol": "R_100",
            "timeframe": 60,  # بالثواني (60, 300, 900)
            "strategy": "scalping",  # scalping, bollinger, sr
            "contract_type": "MULT",  # MULT أو CALLPUT
            "stake": 10.0,
            "multiplier": 100,
            "stop_loss": 5.0,
            "take_profit": 10.0,
            "duration": 5,
            "max_daily_loss": 30.0,
            "max_daily_profit": 50.0,
            "demo_only_lock": True,
            "tg_token": "",
            "tg_chat_id": "",
        }

        # حالة الحساب والسوق
        self.balance = 0.0
        self.currency = "USD"
        self.is_virtual = True
        self.account_id = "---"
        self.daily_pnl = 0.0
        self.consecutive_losses = 0
        self.cooldown_until = 0.0

        # بيانات الشموع والصفقات
        self.candles = []
        self.active_contract_id = None
        self.last_signal_epoch = 0
        self.current_price = 0.0
        self.current_rsi = 50.0
        self.current_ema9 = 0.0
        self.current_ema21 = 0.0
        self.signal_status = "في انتظار التشغيل..."

        # مهام الخلفية
        self._main_task = None
        self._ping_task = None

    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.ui_callback("log", {"time": timestamp, "msg": message, "level": level})

    async def send_telegram(self, text: str):
        token = self.config["tg_token"].strip()
        chat_id = self.config["tg_chat_id"].strip()
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": f"🤖 *Deriv Cyber Bot*\n{text}", "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        self.log(f"فشل إرسال إشعار تيليجرام: {resp.status}", "WARN")
        except Exception as e:
            self.log(f"خطأ في اتصال تيليجرام: {e}", "WARN")

    async def start(self):
        if self.is_running:
            return
        if not self.config["api_token"].strip():
            self.log("يرجى إدخال Deriv API Token في الإعدادات أولاً!", "ERROR")
            return

        self.is_running = True
        self.ui_callback("status", {"running": True})
        self.log("جاري تشغيل محرك التداول والاتصال بـ Deriv...", "INFO")
        self._main_task = asyncio.create_task(self._connection_loop())

    async def stop(self, reason: str = "تم الإيقاف يدوياً من قبل المستخدم"):
        self.is_running = False
        self.is_connected = False
        self.signal_status = "متوقف"
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
        self.log(f"توقف البوت: {reason}", "WARN")
        self.ui_callback("status", {"running": False, "connected": False})
        await self.send_telegram(f"🛑 توقف البوت:\n{reason}")

    async def _ping_loop(self):
        while self.is_running and self.ws:
            try:
                await asyncio.sleep(30)
                if self.ws and self.is_connected:
                    await self.ws.send(json.dumps({"ping": 1}))
            except asyncio.CancelledError:
                break
            except Exception:
                break

    async def _connection_loop(self):
        while self.is_running:
            try:
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    self.ws = ws
                    self.is_connected = True
                    self.ui_callback("status", {"connected": True})
                    self.log("تم الاتصال بخادم Deriv WebSocket بنجاح.", "SUCCESS")

                    self._ping_task = asyncio.create_task(self._ping_loop())

                    # 1. المصادقة
                    await ws.send(json.dumps({"authorize": self.config["api_token"].strip()}))

                    # 2. استقبال الرسائل
                    async for raw_msg in ws:
                        if not self.is_running:
                            break
                        data = json.loads(raw_msg)
                        await self._handle_message(data)

            except websockets.ConnectionClosed:
                self.is_connected = False
                self.ui_callback("status", {"connected": False})
                if self.is_running:
                    self.log("انقطع الاتصال، جاري إعادة الاتصال خلال 5 ثوانٍ...", "WARN")
                    await asyncio.sleep(5)
            except Exception as e:
                self.is_connected = False
                self.ui_callback("status", {"connected": False})
                if self.is_running:
                    self.log(f"خطأ في الاتصال: {e} - إعادة المحاولة خلال 5 ثوانٍ...", "ERROR")
                    await asyncio.sleep(5)

    async def _handle_message(self, data: dict):
        msg_type = data.get("msg_type")

        if "error" in data:
            err_msg = data["error"].get("message", "خطأ غير معروف")
            self.log(f"خطأ من Deriv API: {err_msg}", "ERROR")
            if msg_type == "authorize":
                await self.stop("فشل التحقق من API Token")
            return

        if msg_type == "ping":
            return

        elif msg_type == "authorize":
            auth = data["authorize"]
            self.balance = float(auth["balance"])
            self.currency = auth["currency"]
            self.is_virtual = bool(auth["is_virtual"])
            self.account_id = auth["loginid"]

            if self.config["demo_only_lock"] and not self.is_virtual:
                self.log("تم حظر التداول! الحساب حقيقي وقفل الأمان التجريبي مفعّل.", "ERROR")
                await self.stop("حماية الحساب الحقيقي مفعّلة (Demo Safety Lock)")
                return

            acc_type = "تجريبي (Demo)" if self.is_virtual else "حقيقي (REAL)"
            self.log(f"تم تسجيل الدخول: {self.account_id} [{acc_type}] | الرصيد: {self.balance} {self.currency}", "SUCCESS")
            await self.send_telegram(
                f"🚀 بدأ البوت العمل\nالحساب: `{self.account_id}` ({acc_type})\nالرصيد: `{self.balance} {self.currency}`\nالأصل: `{self.config['symbol']}`"
            )

            await self.ws.send(json.dumps({"balance": 1, "subscribe": 1}))
            await self.ws.send(json.dumps({"proposal_open_contract": 1, "subscribe": 1}))
            await self._subscribe_candles()

        elif msg_type == "balance":
            self.balance = float(data["balance"]["balance"])
            self.ui_callback("account", {})

        elif msg_type == "candles":
            self.candles = [
                {
                    "epoch": c["epoch"],
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                }
                for c in data["candles"]
            ]
            self.log(f"تم تحميل {len(self.candles)} شمعة تاريخية لـ {self.config['symbol']}.", "INFO")
            await self._evaluate_market()

        elif msg_type == "ohlc":
            ohlc = data["ohlc"]
            candle = {
                "epoch": int(ohlc["open_time"]),
                "open": float(ohlc["open"]),
                "high": float(ohlc["high"]),
                "low": float(ohlc["low"]),
                "close": float(ohlc["close"]),
            }
            self.current_price = candle["close"]

            if self.candles and self.candles[-1]["epoch"] == candle["epoch"]:
                self.candles[-1] = candle
            else:
                self.candles.append(candle)
                if len(self.candles) > 200:
                    self.candles.pop(0)

            await self._evaluate_market()

        elif msg_type == "buy":
            buy_info = data["buy"]
            self.active_contract_id = buy_info["contract_id"]
            buy_price = buy_info["buy_price"]
            self.log(f"✅ تم فتح صفقة جديدة | ID: {self.active_contract_id} | المبلغ: {buy_price}$", "SUCCESS")
            await self.send_telegram(
                f"📈 *تم فتح صفقة جديدة*\nالأصل: `{self.config['symbol']}`\nالمبلغ: `{buy_price} {self.currency}`\nID: `{self.active_contract_id}`"
            )

        elif msg_type == "proposal_open_contract":
            poc = data.get("proposal_open_contract", {})
            if not poc:
                return
            cid = poc.get("contract_id")
            is_sold = poc.get("is_sold", 0)

            if not is_sold:
                self.active_contract_id = cid
            else:
                if self.active_contract_id == cid:
                    self.active_contract_id = None

                profit = float(poc.get("profit", 0.0))
                self.daily_pnl = round(self.daily_pnl + profit, 2)

                if profit > 0:
                    self.consecutive_losses = 0
                    self.log(f"💰 ربح الصفقة #{cid}: +{profit}$ | صافي اليوم: {self.daily_pnl}$", "SUCCESS")
                    await self.send_telegram(f"💰 *صفقة رابحة!* `+{profit} {self.currency}`\nصافي اليوم: `{self.daily_pnl} {self.currency}`")
                else:
                    self.consecutive_losses += 1
                    self.log(f"🔻 خسارة الصفقة #{cid}: {profit}$ | صافي اليوم: {self.daily_pnl}$", "ERROR")
                    await self.send_telegram(f"🔻 *صفقة خاسرة:* `{profit} {self.currency}`\nصافي اليوم: `{self.daily_pnl} {self.currency}`")

                self.ui_callback("account", {})
                await self._check_risk_limits()

    async def _subscribe_candles(self):
        req = {
            "ticks_history": self.config["symbol"],
            "adjust_start_time": 1,
            "count": 120,
            "end": "latest",
            "granularity": int(self.config["timeframe"]),
            "style": "candles",
            "subscribe": 1,
        }
        await self.ws.send(json.dumps(req))

    async def _check_risk_limits(self):
        if self.daily_pnl >= self.config["max_daily_profit"]:
            await self.stop(f"🎯 تم تحقيق هدف الربح اليومي ({self.daily_pnl}$)")
            return

        if self.daily_pnl <= -abs(self.config["max_daily_loss"]):
            await self.stop(f"🛑 تم الوصول للحد الأقصى للخسارة اليومية ({self.daily_pnl}$)")
            return

        if self.consecutive_losses >= 3:
            self.cooldown_until = time.time() + (15 * 60)
            self.consecutive_losses = 0
            msg = "❄️ تفعيل التبريد الذكي لمدة 15 دقيقة بعد 3 خسائر متتالية."
            self.log(msg, "WARN")
            await self.send_telegram(msg)

    async def _evaluate_market(self):
        if len(self.candles) < 60:
            return

        closes = [c["close"] for c in self.candles]
        self.current_price = closes[-1]

        ema9_list = TechnicalIndicators.calculate_ema(closes, 9)
        ema21_list = TechnicalIndicators.calculate_ema(closes, 21)
        ema50_list = TechnicalIndicators.calculate_ema(closes, 50)
        rsi = TechnicalIndicators.calculate_rsi(closes, 14)
        bb_upper, bb_mid, bb_lower = TechnicalIndicators.calculate_bollinger_bands(closes, 20, 2.0)

        self.current_rsi = rsi
        self.current_ema9 = round(ema9_list[-1], 4)
        self.current_ema21 = round(ema21_list[-1], 4)
        ema50 = ema50_list[-1]

        if time.time() < self.cooldown_until:
            rem = int((self.cooldown_until - time.time()) / 60) + 1
            self.signal_status = f"❄️ تبريد ذكي ({rem} دقيقة متبقية)"
            self.ui_callback("market", {})
            return

        if self.active_contract_id is not None:
            self.signal_status = f"⏳ صفقة نشطة قيد المراقبة (#{self.active_contract_id})"
            self.ui_callback("market", {})
            return

        current_epoch = self.candles[-1]["epoch"]
        if current_epoch == self.last_signal_epoch:
            self.ui_callback("market", {})
            return

        signal = None
        strategy = self.config["strategy"]

        if strategy == "scalping":
            prev_ema9, curr_ema9 = ema9_list[-2], ema9_list[-1]
            prev_ema21, curr_ema21 = ema21_list[-2], ema21_list[-1]

            if prev_ema9 <= prev_ema21 and curr_ema9 > curr_ema21 and rsi > 55 and self.current_price > ema50:
                signal = "BUY"
            elif prev_ema9 >= prev_ema21 and curr_ema9 < curr_ema21 and rsi < 45 and self.current_price < ema50:
                signal = "SELL"
            self.signal_status = "مراقبة تقاطع EMA + زخم RSI..."

        elif strategy == "bollinger" and bb_lower and bb_upper:
            if self.current_price <= bb_lower and rsi < 30:
                signal = "BUY"
            elif self.current_price >= bb_upper and rsi > 70:
                signal = "SELL"
            self.signal_status = "مراقبة حدود البولينجر + تشبع RSI..."

        elif strategy == "sr":
            supports, resistances = TechnicalIndicators.find_support_resistance(self.candles[:-1], lookback=50, window=3)
            curr_c = self.candles[-1]
            prev_c = self.candles[-2]
            tolerance = self.current_price * 0.0005

            for sup in supports:
                if abs(prev_c["low"] - sup) <= tolerance and curr_c["close"] > curr_c["open"] and rsi < 50:
                    signal = "BUY"
                    break

            if not signal:
                for res in resistances:
                    if abs(prev_c["high"] - res) <= tolerance and curr_c["close"] < curr_c["open"] and rsi > 50:
                        signal = "SELL"
                        break
            self.signal_status = "مراقبة مناطق الدعم والمقاومة الديناميكية..."

        self.ui_callback("market", {})

        if signal:
            self.last_signal_epoch = current_epoch
            self.signal_status = f"🔥 إشارة {signal} مؤكدة! جاري التنفيذ..."
            self.ui_callback("market", {})
            self.log(f"🎯 تم رصد إشارة [{signal}] عبر استراتيجية [{strategy}] عند السعر {self.current_price}", "SUCCESS")
            await self._execute_trade(signal)

    async def _execute_trade(self, direction: str):
        stake = float(self.config["stake"])
        symbol = self.config["symbol"]

        if self.config["contract_type"] == "MULT":
            contract_type = "MULTUP" if direction == "BUY" else "MULTDOWN"
            buy_req = {
                "buy": 1,
                "price": stake,
                "parameters": {
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": contract_type,
                    "currency": self.currency,
                    "multiplier": int(self.config["multiplier"]),
                    "symbol": symbol,
                    "limit_order": {
                        "stop_loss": float(self.config["stop_loss"]),
                        "take_profit": float(self.config["take_profit"]),
                    },
                },
            }
        else:
            contract_type = "CALL" if direction == "BUY" else "PUT"
            buy_req = {
                "buy": 1,
                "price": stake,
                "parameters": {
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": contract_type,
                    "currency": self.currency,
                    "duration": int(self.config["duration"]),
                    "duration_unit": "m",
                    "symbol": symbol,
                },
            }

        try:
            await self.ws.send(json.dumps(buy_req))
        except Exception as e:
            self.log(f"فشل إرسال أمر الشراء: {e}", "ERROR")


# =====================================================================
# 3. واجهة المستخدم الرسومية (Flet Cyber Neon UI)
# =====================================================================
async def main(page: ft.Page):
    BG_DARK = "#0A0E17"
    CARD_BG = "#131B2E"
    NEON_CYAN = "#00F5D4"
    NEON_PURPLE = "#9B5DE5"
    NEON_GREEN = "#00E676"
    NEON_RED = "#FF2A5F"
    TEXT_MUTED = "#8A99B8"

    page.title = "Deriv Cyber Neon Bot"
    page.bgcolor = BG_DARK
    page.theme_mode = ft.ThemeMode.DARK
    page.padding = 0
    page.rtl = True

    # شاشة التحميل المتحركة (Splash Screen)
    splash_icon = ft.Icon(ft.Icons.AUTO_GRAPH_ROUNDED, size=80, color=NEON_CYAN)
    splash_progress = ft.ProgressBar(width=220, color=NEON_PURPLE, bgcolor=CARD_BG)
    splash_view = ft.Container(
        expand=True,
        bgcolor=BG_DARK,
        alignment=ft.alignment.center,
        content=ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=15,
            controls=[
                splash_icon,
                ft.Text("DERIV CYBER BOT", size=26, weight=ft.FontWeight.BOLD, color=NEON_CYAN),
                ft.Text("Algorithmic Trading Engine v3.0", size=13, color=NEON_PURPLE),
                splash_progress,
            ],
        ),
    )
    page.add(splash_view)
    await asyncio.sleep(1.8)
    page.controls.clear()

    # عناصر لوحة التحكم
    txt_conn_badge = ft.Text("غير متصل", size=12, weight=ft.FontWeight.BOLD, color=NEON_RED)
    conn_dot = ft.Container(width=10, height=10, border_radius=5, bgcolor=NEON_RED)

    txt_balance = ft.Text("0.00 USD", size=26, weight=ft.FontWeight.BOLD, color="#FFFFFF")
    txt_acc_type = ft.Text("DEMO MODE", size=11, weight=ft.FontWeight.BOLD, color=NEON_CYAN)
    txt_daily_pnl = ft.Text("0.00 $", size=18, weight=ft.FontWeight.BOLD, color=NEON_GREEN)

    txt_symbol_title = ft.Text("R_100 (1m)", size=16, weight=ft.FontWeight.BOLD, color=NEON_PURPLE)
    txt_live_price = ft.Text("0.0000", size=22, weight=ft.FontWeight.BOLD, color=NEON_CYAN)
    txt_rsi_val = ft.Text("RSI: --", size=13, color="#FFFFFF")
    txt_ema_val = ft.Text("EMA 9/21: -- / --", size=13, color="#FFFFFF")
    txt_signal_state = ft.Text("في انتظار التشغيل...", size=13, color=TEXT_MUTED, italic=True)

    logs_list = ft.ListView(expand=True, spacing=6, auto_scroll=True)

    def on_engine_event(event_type: str, payload: dict):
        if event_type == "log":
            lvl = payload["level"]
            if lvl == "SUCCESS":
                color = NEON_GREEN
            elif lvl == "ERROR":
                color = NEON_RED
            elif lvl == "WARN":
                color = "#FFB703"
            else:
                color = NEON_CYAN

            logs_list.controls.append(
                ft.Container(
                    padding=8,
                    border_radius=8,
                    bgcolor="#0D1322",
                    border=ft.border.all(1, "#1E2942"),
                    content=ft.Text(f"[{payload['time']}] {payload['msg']}", size=12, color=color),
                )
            )
            if len(logs_list.controls) > 100:
                logs_list.controls.pop(0)

        elif event_type == "status":
            if "connected" in payload:
                is_c = payload["connected"]
                txt_conn_badge.value = "متصل (Live)" if is_c else "غير متصل"
                txt_conn_badge.color = NEON_GREEN if is_c else NEON_RED
                conn_dot.bgcolor = NEON_GREEN if is_c else NEON_RED
            if "running" in payload:
                btn_start.disabled = payload["running"]
                btn_stop.disabled = not payload["running"]

        elif event_type == "account":
            txt_balance.value = f"{engine.balance:,.2f} {engine.currency}"
            txt_acc_type.value = "حساب تجريبي (DEMO)" if engine.is_virtual else "⚠️ حساب حقيقي (REAL)"
            txt_acc_type.color = NEON_CYAN if engine.is_virtual else NEON_RED
            pnl = engine.daily_pnl
            txt_daily_pnl.value = f"{'+' if pnl > 0 else ''}{pnl:.2f} {engine.currency}"
            txt_daily_pnl.color = NEON_GREEN if pnl >= 0 else NEON_RED

        elif event_type == "market":
            tf_map = {60: "1m", 300: "5m", 900: "15m"}
            tf_str = tf_map.get(int(engine.config["timeframe"]), "1m")
            txt_symbol_title.value = f"{engine.config['symbol']} ({tf_str})"
            txt_live_price.value = f"{engine.current_price}"
            txt_rsi_val.value = f"RSI (14): {engine.current_rsi}"
            txt_ema_val.value = f"EMA 9/21: {engine.current_ema9} / {engine.current_ema21}"
            txt_signal_state.value = engine.signal_status

        page.update()

    engine = DerivTradingEngine(ui_callback=on_engine_event)

    async def handle_start(e):
        save_settings_to_engine()
        await engine.start()

    async def handle_stop(e):
        await engine.stop("تم الضغط على زر الإيقاف الفوري")

    btn_start = ft.ElevatedButton(
        text="تشغيل البوت (START)",
        icon=ft.Icons.PLAY_ARROW_ROUNDED,
        bgcolor=NEON_GREEN,
        color="#05080F",
        height=48,
        expand=True,
        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)),
        on_click=handle_start,
    )

    btn_stop = ft.ElevatedButton(
        text="إيقاف فوري (STOP)",
        icon=ft.Icons.STOP_ROUNDED,
        bgcolor=NEON_RED,
        color="#FFFFFF",
        height=48,
        expand=True,
        disabled=True,
        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=12)),
        on_click=handle_stop,
    )

    # 1. تبويب لوحة التحكم
    dashboard_tab = ft.Container(
        padding=15,
        content=ft.Column(
            scroll=ft.ScrollMode.AUTO,
            spacing=14,
            controls=[
                ft.Container(
                    padding=16,
                    border_radius=16,
                    bgcolor=CARD_BG,
                    border=ft.border.all(1, NEON_PURPLE),
                    content=ft.Column(
                        spacing=8,
                        controls=[
                            ft.Row(
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                                controls=[
                                    ft.Container(
                                        padding=ft.padding.symmetric(horizontal=10, vertical=4),
                                        border_radius=8,
                                        bgcolor="#1C263E",
                                        content=txt_acc_type,
                                    ),
                                    ft.Row(spacing=6, controls=[conn_dot, txt_conn_badge]),
                                ],
                            ),
                            ft.Text("الرصيد المتاح", size=12, color=TEXT_MUTED),
                            txt_balance,
                            ft.Divider(height=1, color="#23304D"),
                            ft.Row(
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                                controls=[
                                    ft.Text("صافي ربح/خسارة اليوم:", size=13, color=TEXT_MUTED),
                                    txt_daily_pnl,
                                ],
                            ),
                        ],
                    ),
                ),
                ft.Container(
                    padding=16,
                    border_radius=16,
                    bgcolor=CARD_BG,
                    border=ft.border.all(1, NEON_CYAN),
                    content=ft.Column(
                        spacing=10,
                        controls=[
                            ft.Row(
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                                controls=[txt_symbol_title, txt_live_price],
                            ),
                            ft.Row(
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                                controls=[txt_rsi_val, txt_ema_val],
                            ),
                            ft.Container(
                                padding=10,
                                border_radius=10,
                                bgcolor="#0D1322",
                                content=ft.Row(
                                    spacing=8,
                                    controls=[
                                        ft.Icon(ft.Icons.RADAR_ROUNDED, color=NEON_CYAN, size=18),
                                        txt_signal_state,
                                    ],
                                ),
                            ),
                        ],
                    ),
                ),
                ft.Row(spacing=12, controls=[btn_start, btn_stop]),
            ],
        ),
    )

    # 2. تبويب الإعدادات
    inp_token = ft.TextField(
        label="Deriv API Token",
        password=True,
        can_reveal_password=True,
        border_color=NEON_PURPLE,
        text_size=13,
    )
    dd_symbol = ft.Dropdown(
        label="الأصل المالي (Symbol)",
        value="R_100",
        border_color=NEON_CYAN,
        options=[
            ft.dropdown.Option("R_100", "Volatility 100 Index (R_100)"),
            ft.dropdown.Option("R_75", "Volatility 75 Index (R_75)"),
            ft.dropdown.Option("R_50", "Volatility 50 Index (R_50)"),
            ft.dropdown.Option("1HZ100V", "Volatility 100 (1s) Index"),
            ft.dropdown.Option("1HZ75V", "Volatility 75 (1s) Index"),
            ft.dropdown.Option("frxEURUSD", "EUR/USD Forex"),
            ft.dropdown.Option("frxXAUUSD", "Gold / USD (XAUUSD)"),
        ],
    )
    dd_timeframe = ft.Dropdown(
        label="الإطار الزمني (Timeframe)",
        value="60",
        border_color=NEON_CYAN,
        options=[
            ft.dropdown.Option("60", "1 دقيقة (1m)"),
            ft.dropdown.Option("300", "5 دقائق (5m)"),
            ft.dropdown.Option("900", "15 دقيقة (15m)"),
        ],
    )
    dd_strategy = ft.Dropdown(
        label="الاستراتيجية الخوارزمية",
        value="scalping",
        border_color=NEON_PURPLE,
        options=[
            ft.dropdown.Option("scalping", "1. السكالبينج السريع (EMA 9/21/50 + RSI)"),
            ft.dropdown.Option("bollinger", "2. ارتداد البولينجر باند (BB + RSI)"),
            ft.dropdown.Option("sr", "3. الدعوم والمقاومات (Support & Resistance)"),
        ],
    )
    dd_contract = ft.Dropdown(
        label="نوع العقد (Contract Type)",
        value="MULT",
        border_color=NEON_CYAN,
        options=[
            ft.dropdown.Option("MULT", "المضاعفات (Multipliers - SL/TP)"),
            ft.dropdown.Option("CALLPUT", "صعود / هبوط (Rise / Fall)"),
        ],
    )
    inp_stake = ft.TextField(label="مبلغ الدخول (Stake $)", value="10", keyboard_type=ft.KeyboardType.NUMBER, expand=True)
    inp_mult = ft.TextField(label="الرافعة (Multiplier)", value="100", keyboard_type=ft.KeyboardType.NUMBER, expand=True)
    inp_sl = ft.TextField(label="وقف الخسارة (SL $)", value="5", keyboard_type=ft.KeyboardType.NUMBER, expand=True)
    inp_tp = ft.TextField(label="جني الأرباح (TP $)", value="10", keyboard_type=ft.KeyboardType.NUMBER, expand=True)
    inp_max_loss = ft.TextField(label="أقصى خسارة يومية ($)", value="30", keyboard_type=ft.KeyboardType.NUMBER, expand=True)
    inp_max_profit = ft.TextField(label="هدف الربح اليومي ($)", value="50", keyboard_type=ft.KeyboardType.NUMBER, expand=True)

    def on_demo_lock_change(e):
        if not sw_demo_lock.value:
            def confirm_real(ev):
                dlg_warning.open = False
                page.update()

            def cancel_real(ev):
                sw_demo_lock.value = True
                dlg_warning.open = False
                page.update()

            dlg_warning = ft.AlertDialog(
                modal=True,
                title=ft.Text("⚠️ تحذير أمان: حساب حقيقي!", color=NEON_RED, weight=ft.FontWeight.BOLD),
                content=ft.Text(
                    "أنت على وشك إلغاء قفل الحماية التجريبي والسماح للبوت بالتداول بأموال حقيقية.\n"
                    "تأكد من ضبط حدود وقف الخسارة اليومية بدقة قبل المتابعة."
                ),
                actions=[
                    ft.TextButton("إلغاء (البقاء في الوضع الآمن)", on_click=cancel_real),
                    ft.ElevatedButton("أتحمل المسؤولية، متابعة", bgcolor=NEON_RED, color="#FFFFFF", on_click=confirm_real),
                ],
            )
            page.overlay.append(dlg_warning)
            dlg_warning.open = True
            page.update()

    sw_demo_lock = ft.Switch(
        label="قفل الحماية التجريبي (Demo Safety Lock)",
        value=True,
        active_color=NEON_GREEN,
        on_change=on_demo_lock_change,
    )

    inp_tg_token = ft.TextField(label="Telegram Bot Token (اختياري)", password=True, can_reveal_password=True, text_size=12)
    inp_tg_chat = ft.TextField(label="Telegram Chat ID (اختياري)", text_size=12)

    def save_settings_to_engine(e=None):
        try:
            engine.config["api_token"] = inp_token.value.strip()
            engine.config["symbol"] = dd_symbol.value
            engine.config["timeframe"] = int(dd_timeframe.value)
            engine.config["strategy"] = dd_strategy.value
            engine.config["contract_type"] = dd_contract.value
            engine.config["stake"] = float(inp_stake.value or 10)
            engine.config["multiplier"] = int(inp_mult.value or 100)
            engine.config["stop_loss"] = float(inp_sl.value or 5)
            engine.config["take_profit"] = float(inp_tp.value or 10)
            engine.config["max_daily_loss"] = float(inp_max_loss.value or 30)
            engine.config["max_daily_profit"] = float(inp_max_profit.value or 50)
            engine.config["demo_only_lock"] = sw_demo_lock.value
            engine.config["tg_token"] = inp_tg_token.value.strip()
            engine.config["tg_chat_id"] = inp_tg_chat.value.strip()
            if e:
                engine.log("تم حفظ الإعدادات وتحديث معايير المحرك بنجاح.", "SUCCESS")
                page.open(ft.SnackBar(ft.Text("تم حفظ الإعدادات بنجاح!"), bgcolor=NEON_PURPLE))
                page.update()
        except ValueError:
            engine.log("تأكد من إدخال أرقام صحيحة في حقول المبالغ!", "ERROR")

    settings_tab = ft.Container(
        padding=15,
        content=ft.Column(
            scroll=ft.ScrollMode.AUTO,
            spacing=12,
            controls=[
                ft.Text("إعدادات الاتصال والاستراتيجية", size=16, weight=ft.FontWeight.BOLD, color=NEON_CYAN),
                inp_token,
                dd_symbol,
                dd_timeframe,
                dd_strategy,
                dd_contract,
                ft.Row(spacing=10, controls=[inp_stake, inp_mult]),
                ft.Row(spacing=10, controls=[inp_sl, inp_tp]),
                ft.Divider(color="#23304D"),
                ft.Text("إدارة المخاطر والأمان", size=16, weight=ft.FontWeight.BOLD, color=NEON_PURPLE),
                ft.Row(spacing=10, controls=[inp_max_loss, inp_max_profit]),
                sw_demo_lock,
                ft.Divider(color="#23304D"),
                ft.Text("إشعارات تيليجرام", size=15, weight=ft.FontWeight.BOLD, color=NEON_CYAN),
                inp_tg_token,
                inp_tg_chat,
                ft.ElevatedButton(
                    text="حفظ الإعدادات",
                    icon=ft.Icons.SAVE_ROUNDED,
                    bgcolor=NEON_PURPLE,
                    color="#FFFFFF",
                    height=45,
                    width=400,
                    on_click=save_settings_to_engine,
                ),
            ],
        ),
    )

    # 3. تبويب السجل المباشر
    logs_tab = ft.Container(
        padding=12,
        content=ft.Column(
            expand=True,
            controls=[
                ft.Row(
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    controls=[
                        ft.Text("السجل المباشر للعمليات (Live Logs)", size=15, weight=ft.FontWeight.BOLD, color=NEON_CYAN),
                        ft.IconButton(
                            icon=ft.Icons.DELETE_SWEEP_ROUNDED,
                            icon_color=NEON_RED,
                            tooltip="مسح السجل",
                            on_click=lambda _: (logs_list.controls.clear(), page.update()),
                        ),
                    ],
                ),
                ft.Container(
                    expand=True,
                    padding=8,
                    border_radius=12,
                    bgcolor=CARD_BG,
                    border=ft.border.all(1, "#1E2942"),
                    content=logs_list,
                ),
            ],
        ),
    )

    # شريط علوي + التبويبات الرئيسية
    header = ft.Container(
        padding=ft.padding.symmetric(horizontal=16, vertical=12),
        bgcolor=CARD_BG,
        content=ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            controls=[
                ft.Row(
                    spacing=8,
                    controls=[
                        ft.Icon(ft.Icons.BOLT_ROUNDED, color=NEON_CYAN, size=26),
                        ft.Text("DERIV CYBER BOT", size=18, weight=ft.FontWeight.BOLD, color="#FFFFFF"),
                    ],
                ),
                ft.Text("v3.0", size=12, color=NEON_PURPLE, weight=ft.FontWeight.BOLD),
            ],
        ),
    )

    tabs = ft.Tabs(
        selected_index=0,
        animation_duration=250,
        indicator_color=NEON_CYAN,
        label_color=NEON_CYAN,
        unselected_label_color=TEXT_MUTED,
        expand=True,
        tabs=[
            ft.Tab(text="لوحة التحكم", icon=ft.Icons.DASHBOARD_ROUNDED, content=dashboard_tab),
            ft.Tab(text="الإعدادات", icon=ft.Icons.TUNE_ROUNDED, content=settings_tab),
            ft.Tab(text="السجل الحي", icon=ft.Icons.TERMINAL_ROUNDED, content=logs_tab),
        ],
    )

    page.add(ft.Column(expand=True, spacing=0, controls=[header, tabs]))


if __name__ == "__main__":
    if hasattr(ft, "app"):
        ft.app(target=main)
    else:
        ft.run(main, view=ft.AppView.WEB_BROWSER, port=8550)
