
import os
import json
import time
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from statistics import mean, pstdev

import pandas as pd
import requests
import websocket
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

load_dotenv()

# DEMO only
BASE_URL = "https://demo-api-capital.backend-capital.com"
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"

API_KEY = os.getenv("CAP_API_KEY", "")
IDENTIFIER = os.getenv("CAP_IDENTIFIER", "")
API_PASSWORD = os.getenv("CAP_API_PASSWORD", "")

EPIC = "US100"
SIZE = float(os.getenv("CAP_SIZE", "0.1"))
STOP_DISTANCE = 20
PROFIT_DISTANCE = 18

ENABLE_TRADING = (
    os.getenv("ENABLE_DEMO_TRADING", "false").lower() == "true"
)

BALANCE_INTERVAL = 300
BALANCE_LOG_FILE = Path(__file__).resolve().parent / "historial_saldo.txt"
BOLLINGER_LOG_FILE = Path(__file__).resolve().parent / "historial_bollinger.jsonl"
STATE_FILE = Path("last_signal_minute.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


class CapitalBot:
    def __init__(self):
        if not all([API_KEY, IDENTIFIER, API_PASSWORD]):
            raise RuntimeError("Faltan credenciales en .env")

        if SIZE <= 0:
            raise RuntimeError("CAP_SIZE debe ser mayor que cero")

        self.session = requests.Session()
        self.session.headers.update({"X-CAP-API-KEY": API_KEY})

        self.login()

        # Últimos cierres confirmados, usados para las bandas.
        self.closed_closes = []

        # Vela construida con cotizaciones WebSocket.
        self.current_candle = None

        # No operar la vela parcial existente al iniciar o reconectar.
        self.allow_new_candle = False

        self.last_triggered_minute = (
            STATE_FILE.read_text().strip()
            if STATE_FILE.exists()
            else None
        )

        self.order_lock = threading.Lock()
        self.order_in_flight = False

        self.ws_app = None
        self.ws_keepalive_stop = threading.Event()

        

        # Control del vigilante Take Profit
        self.take_profit_position = None
        self.take_profit_lock = threading.Lock()
        self.take_profit_last_attempt = 0.0
        self.take_profit_closing = False

        # Control del vigilante independiente de Stop Loss de emergencia.
        # Este monitor consulta posiciones y cotizaciones por REST, incluso
        # cuando el WebSocket está desconectado.
        self.stop_monitor_lock = threading.Lock()
        self.emergency_closing_ids = set()
        self.stop_monitor_interval = 2.0

    def move_stop_to_breakeven(self, deal_id, entry_price):
        # Esperar un minuto desde la apertura confirmada.
        time.sleep(60)

        attempt = 0

        while True:
            attempt += 1

            try:
                # Comprobar que la posición sigue abierta.
                position_data = self.request(
                    "GET",
                    f"/api/v1/positions/{deal_id}",
                )

                if not position_data.get("position"):
                    logging.info(
                        "Posición cerrada. Se cancela el ajuste "
                        "del Stop Loss."
                    )
                    return

                logging.info(
                    "Intento %d: moviendo Stop Loss a break-even "
                    "| precio de entrada=%.2f",
                    attempt,
                    entry_price,
                )

                # Modificar únicamente el Stop Loss.
                result = self.request(
                    "PUT",
                    f"/api/v1/positions/{deal_id}",
                    json={
                        "stopLevel": float(entry_price)
                    },
                )

                update_reference = result.get("dealReference")

                if not update_reference:
                    logging.warning(
                        "La modificación no devolvió dealReference."
                    )
                else:
                    confirmation = self.request(
                        "GET",
                        f"/api/v1/confirms/{update_reference}",
                    )

                    if confirmation.get("dealStatus") == "ACCEPTED":
                        logging.info(
                            "STOP LOSS MODIFICADO A BREAK-EVEN "
                            "| entrada=%.2f | intentos=%d",
                            entry_price,
                            attempt,
                        )
                        return

                    logging.warning(
                        "Modificación no aceptada: %s",
                        confirmation,
                    )

            except RuntimeError as exc:
                if "error.not-found.dealId" in str(exc):
                    logging.error(
                        "El dealId %s no existe en Capital.com. "
                        "Se detiene este proceso de break-even.",
                        deal_id,
                    )
                    return

                logging.exception(
                    "Error en intento %d de mover Stop Loss",
                    attempt,
                )

            except Exception:
                logging.exception(
                    "Error inesperado en intento %d de mover Stop Loss",
                    attempt,
                )

            logging.info(
                "Nuevo intento de break-even dentro de 60 segundos."
            )

            time.sleep(60)

    def login(self):
        response = self.session.post(
            f"{BASE_URL}/api/v1/session",
            json={
                "identifier": IDENTIFIER,
                "password": API_PASSWORD,
                "encryptedPassword": False,
            },
            timeout=20,
        )
        response.raise_for_status()

        cst = response.headers.get("CST")
        security = response.headers.get("X-SECURITY-TOKEN")

        if not cst or not security:
            raise RuntimeError("No se recibieron tokens de sesión")

        self.cst = cst
        self.security_token = security

        self.session.headers.update({
            "CST": cst,
            "X-SECURITY-TOKEN": security,
        })

        logging.info("Sesión DEMO autenticada")

    def request(self, method, path, **kwargs):
        response = self.session.request(
            method,
            BASE_URL + path,
            timeout=20,
            **kwargs,
        )

        if response.status_code == 401:
            self.login()
            response = self.session.request(
                method,
                BASE_URL + path,
                timeout=20,
                **kwargs,
            )

        if not response.ok:
            raise RuntimeError(
                f"API HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

        if not response.content:
            return {}

        return response.json()

    
    
    
    def save_entry_bollinger(self, deal_id, deal_reference, direction,
                              entry_price, upper, lower):
        """Guarda las bandas exactas calculadas al confirmar la apertura."""
        record = {
            "dealId": deal_id,
            "dealReference": deal_reference,
            "timestampUtc": datetime.now(timezone.utc).isoformat(),
            "direction": direction,
            "size": SIZE,
            "entryPrice": float(entry_price),
            "upperBand": float(upper),
            "lowerBand": float(lower),
        }
        try:
            with BOLLINGER_LOG_FILE.open("a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\\n")
            logging.info(
                "Bandas de Bollinger guardadas | dealId=%s | superior=%.2f | inferior=%.2f",
                deal_id, upper, lower,
            )
        except OSError:
            logging.exception("No se pudieron guardar las bandas de Bollinger.")

    @staticmethod
    def load_entry_bollinger():
        """Lee el historial local de bandas."""
        records = []
        if not BOLLINGER_LOG_FILE.exists():
            return records
        try:
            with BOLLINGER_LOG_FILE.open("r", encoding="utf-8") as file:
                for line in file:
                    try:
                        item = json.loads(line)
                        if isinstance(item, dict):
                            records.append(item)
                    except json.JSONDecodeError:
                        logging.warning("Línea inválida en %s", BOLLINGER_LOG_FILE)
        except OSError:
            logging.exception("No se pudo leer %s", BOLLINGER_LOG_FILE)
        return records

    @staticmethod
    def find_entry_bollinger(transaction, records):
        """Relaciona una transacción con sus bandas guardadas."""
        transaction_ids = {
            str(transaction.get(key))
            for key in ("dealId", "reference", "dealReference", "deal_id")
            if transaction.get(key) not in (None, "")
        }
        for record in reversed(records):
            record_ids = {
                str(record.get(key))
                for key in ("dealId", "dealReference")
                if record.get(key) not in (None, "")
            }
            if transaction_ids & record_ids:
                return record

        # Si la API no incluye el dealId, intentar por hora y tamaño.
        raw_date = transaction.get("dateUtc") or transaction.get("date")
        if not raw_date:
            return None
        try:
            tx_time = datetime.fromisoformat(
                raw_date.replace("Z", "+00:00")
            )
            if tx_time.tzinfo is None:
                tx_time = tx_time.replace(tzinfo=timezone.utc)
            tx_time = tx_time.astimezone(timezone.utc)
        except (ValueError, AttributeError, TypeError):
            return None

        tx_size = transaction.get("size")
        candidates = []
        for record in records:
            try:
                record_time = datetime.fromisoformat(
                    record["timestampUtc"].replace("Z", "+00:00")
                ).astimezone(timezone.utc)
            except (KeyError, ValueError, AttributeError, TypeError):
                continue
            seconds = abs((tx_time - record_time).total_seconds())
            if seconds > 120:
                continue
            try:
                if tx_size is not None and abs(
                    abs(float(tx_size)) - float(record.get("size", SIZE))
                ) > 1e-8:
                    continue
            except (ValueError, TypeError):
                pass
            candidates.append((seconds, record))
        return min(candidates, key=lambda item: item[0])[1] if candidates else None

    def show_last_five_operations(self):
        from datetime import datetime, timedelta, timezone
        from zoneinfo import ZoneInfo

        bogota = ZoneInfo("America/Bogota")
        now = datetime.now(timezone.utc)

        # Últimos cinco días calendario, incluyendo hoy.
        today = now.astimezone(bogota).date()
        first_day = today - timedelta(days=4)

        def fecha_colombia(value):
            if not value:
                return "N/D"
            try:
                dt = datetime.fromisoformat(
                    value.replace("Z", "+00:00")
                )
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(bogota).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            except (ValueError, AttributeError):
                return str(value)

        def consultar(path, inicio, fin):
            return self.request(
                "GET",
                path,
                params={
                    "from": inicio.strftime("%Y-%m-%dT%H:%M:%S"),
                    "to": fin.strftime("%Y-%m-%dT%H:%M:%S"),
                    "detailed": "true",
                },
            )

        print("\n=== HISTORIAL DEMO: ÚLTIMOS 5 DÍAS ===")
        print("Horas mostradas en Colombia (UTC-5).")
        print(
            f"Periodo: {first_day} hasta {today}"
        )

        print("\n=== TRANSACCIONES Y MOVIMIENTOS FINANCIEROS ===")

        transactions = []
        seen = set()
        bollinger_records = self.load_entry_bollinger()

        for offset in range(5):
            day = first_day + timedelta(days=offset)

            # Límites de cada día en UTC.
            inicio_local = datetime(
                day.year, day.month, day.day,
                tzinfo=bogota
            )
            fin_local = inicio_local + timedelta(days=1)

            inicio_utc = inicio_local.astimezone(timezone.utc)
            fin_utc = min(
                fin_local.astimezone(timezone.utc),
                now
            )

            if inicio_utc >= now:
                continue

            print(f"\nConsultando día: {day}")

            result = consultar(
                "/api/v1/history/transactions",
                inicio_utc,
                fin_utc,
            )

            daily_transactions = result.get("transactions", [])

            print(
                f"Transacciones recibidas: {len(daily_transactions)}"
            )

            for t in daily_transactions:
                key = (
                    t.get("dateUtc") or t.get("date"),
                    t.get("transactionType"),
                    t.get("reference"),
                    t.get("size"),
                )

                if key not in seen:
                    seen.add(key)
                    transactions.append(t)

        transactions.sort(
            key=lambda t: t.get(
                "dateUtc", t.get("date", "")
            ),
            reverse=True,
        )

        if not transactions:
            print("No se encontraron transacciones.")
        else:
            for t in transactions:
                print("\n--------------------------------")
                print(
                    "Fecha:",
                    fecha_colombia(
                        t.get("dateUtc") or t.get("date")
                    ),
                )
                print("Tamaño:", t.get("size", "N/D"))

                bollinger = self.find_entry_bollinger(t, bollinger_records)
                if bollinger:
                    print("Banda superior al abrir:", f'{bollinger["upperBand"]:.2f}')
                    print("Banda inferior al abrir:", f'{bollinger["lowerBand"]:.2f}')
                else:
                    print("Bandas de Bollinger al abrir: sin registro guardado")

                financial_keys = (
                    "profit",
                    "profitAndLoss",
                    "grossProfit",
                    "netProfit",
                    "amount",
                    "cashflow",
                    "balance",
                    "swap",
                    "commission",
                )

                found = False

                for key in financial_keys:
                    if key in t:
                        print(f"{key}: {t[key]}")
                        found = True

                if not found:
                    print(
                        "Resultado monetario por operación: "
                        "no incluido en esta respuesta de la API."
                    )

        print("\n=== RESUMEN ===")
        print(f"Transacciones: {len(transactions)}")




    
    
    

    @staticmethod        
    def midpoint(value):
        if isinstance(value, dict):
            bid = value.get("bid")
            ask = value.get("ask")

            if bid is not None and ask is not None:
                return (float(bid) + float(ask)) / 2

            value = bid if bid is not None else ask

        return float(value) if value is not None else None

    def seed_history(self):
        """Carga cierres históricos agrupados en velas de 2 minutos."""
        data = self.request(
            "GET",
            f"/api/v1/prices/{EPIC}",
            params={"resolution": "MINUTE", "max": 100},
        )

        rows = []

        for item in data.get("prices", []):
            timestamp = (
                item.get("snapshotTimeUTC")
                or item.get("snapshotTime")
            )
            close = self.midpoint(item.get("closePrice"))

            if not timestamp or close is None:
                continue

            try:
                ts = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                logging.warning(
                    "Timestamp histórico inválido: %s", timestamp
                )
                continue

            rows.append((ts, close))

        rows.sort(key=lambda row: row[0])

        if len(rows) < 40:
            raise RuntimeError(
                "Historial insuficiente para Bollinger de 2 minutos"
            )

        # Excluir la última cotización por precaución.
        rows = rows[:-1]

        # Agrupar cada par de minutos: 00-01, 02-03, 04-05...
        groups = {}

        for ts, close in rows:
            bucket_minute = (ts.minute // 2) * 2

            bucket = ts.replace(
                minute=bucket_minute,
                second=0,
                microsecond=0,
            )

            minute_offset = ts.minute - bucket_minute

            if minute_offset not in (0, 1):
                continue

            if bucket not in groups:
                groups[bucket] = {}

            groups[bucket][minute_offset] = close

        # Solo conservar grupos completos.
        completed_closes = []

        for bucket in sorted(groups):
            group = groups[bucket]

            if 0 in group and 1 in group:
                # El cierre de la segunda vela es el cierre
                # de la vela sintética de dos minutos.
                completed_closes.append(group[1])

        if len(completed_closes) < 19:
            raise RuntimeError(
                "No hay 19 velas completas de 2 minutos "
                "para inicializar Bollinger"
            )

        self.closed_closes = completed_closes[-19:]

        logging.info(
            "Historial inicializado con %d cierres de 2 minutos",
            len(self.closed_closes),
        )

    def bands(self, current_price):
        """19 cierres de 2 minutos + precio actual."""
        values = self.closed_closes[-19:] + [current_price]

        if len(values) < 20:
            return None, None, None

        middle = mean(values)
        std = pstdev(values)

        return middle, middle + 2 * std, middle - 2 * std

    
    def has_open_position(self):
        data = self.request("GET", "/api/v1/positions")
        positions = data.get("positions", [])

        logging.info("Diagnóstico: posiciones abiertas = %s", len(positions))

        for p in positions:
            market = p.get("market", {})
            position = p.get("position", {})

            logging.info(
                "Posición detectada: epic=%s | dirección=%s | tamaño=%s",
                market.get("epic"),
                position.get("direction"),
                position.get("size"),
            )

        return any(
            p.get("market", {}).get("epic") == EPIC
            for p in positions
        )

    def get_rest_market_quote(self):
        """Obtiene bid/offer actuales mediante REST (independiente del WS)."""
        data = self.request("GET", f"/api/v1/markets/{EPIC}")

        # La respuesta de Capital.com puede venir como:
        # {"instrument": {...}, "snapshot": {...}}
        # o como {"instrument": {"snapshot": {...}}}, según el formato.
        # En el formato recibido en los registros, la cotización está en
        # data["snapshot"] y también puede estar anidada en instrument.
        candidates = []

        if isinstance(data, dict):
            if isinstance(data.get("snapshot"), dict):
                candidates.append(data["snapshot"])

            instrument = data.get("instrument")
            if isinstance(instrument, dict):
                if isinstance(instrument.get("snapshot"), dict):
                    candidates.append(instrument["snapshot"])

            markets = data.get("markets") or []
            for item in markets:
                if isinstance(item, dict):
                    if isinstance(item.get("snapshot"), dict):
                        candidates.append(item["snapshot"])
                    candidates.append(item)

            if isinstance(data.get("market"), dict):
                market = data["market"]
                if isinstance(market.get("snapshot"), dict):
                    candidates.append(market["snapshot"])
                candidates.append(market)

            candidates.append(data)

        quote = next(
            (
                item for item in candidates
                if item.get("bid") is not None
                and item.get("offer") is not None
            ),
            None,
        )

        if quote is None:
            raise RuntimeError(
                f"La API no devolvió bid/offer para {EPIC}. "
                f"Claves recibidas: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}"
            )

        return float(quote["bid"]), float(quote["offer"])

    def position_is_open(self, deal_id):
        """Comprueba el estado real de un deal en la lista de posiciones."""
        data = self.request("GET", "/api/v1/positions")
        return any(
            item.get("position", {}).get("dealId") == deal_id
            for item in data.get("positions", [])
        )

    def close_position_emergency(self, deal_id, direction, stop_level):
        """Cierra una posición por SL y verifica que desapareció de abiertas."""
        with self.stop_monitor_lock:
            if deal_id in self.emergency_closing_ids:
                return
            self.emergency_closing_ids.add(deal_id)

        try:
            logging.critical(
                "STOP LOSS ALCANZADO: enviando cierre de emergencia "
                "| %s | dealId=%s | stop=%.2f",
                direction,
                deal_id,
                stop_level,
            )

            result = self.request(
                "DELETE",
                f"/api/v1/positions/{deal_id}",
            )
            reference = result.get("dealReference")

            if reference:
                confirmation = self.request(
                    "GET", f"/api/v1/confirms/{reference}"
                )
                logging.info(
                    "Confirmación de cierre de emergencia: %s",
                    confirmation,
                )
                if confirmation.get("dealStatus") != "ACCEPTED":
                    logging.error(
                        "Capital.com no confirmó ACCEPTED el cierre "
                        "de %s; se volverá a comprobar.",
                        deal_id,
                    )

            # No se considera cerrado solo por recibir HTTP 200 o reference.
            if not self.position_is_open(deal_id):
                logging.critical(
                    "CIERRE DE EMERGENCIA VERIFICADO | dealId=%s",
                    deal_id,
                )
                return

            logging.error(
                "La posición %s sigue abierta después del intento de cierre. "
                "Se reintentará en el siguiente ciclo.",
                deal_id,
            )

        except RuntimeError as exc:
            # Un 404 puede significar que el broker ya la cerró por su SL/TP.
            # Se consulta la lista antes de decidir que está cerrada.
            if "error.not-found.dealId" in str(exc):
                try:
                    if not self.position_is_open(deal_id):
                        logging.info(
                            "La posición %s ya no aparece abierta; "
                            "probablemente fue cerrada por Capital.com.",
                            deal_id,
                        )
                        return
                except Exception:
                    logging.exception(
                        "No se pudo verificar el estado de %s tras HTTP 404",
                        deal_id,
                    )
            logging.exception(
                "Falló el cierre de emergencia de %s; se reintentará.",
                deal_id,
            )

        except Exception:
            logging.exception(
                "Error inesperado al cerrar por Stop Loss: %s",
                deal_id,
            )

        finally:
            with self.stop_monitor_lock:
                self.emergency_closing_ids.discard(deal_id)

    def stop_loss_monitor_loop(self):
        """Vigila el stopLevel real usando REST aunque el WS falle."""
        logging.info(
            "Vigilante REST de Stop Loss iniciado para %s "
            "(intervalo %.1f s)",
            EPIC,
            self.stop_monitor_interval,
        )

        while True:
            try:
                positions_data = self.request("GET", "/api/v1/positions")
                positions = positions_data.get("positions", [])

                # No hace falta consultar el mercado si no hay posición US100.
                us100_positions = [
                    item for item in positions
                    if item.get("market", {}).get("epic") == EPIC
                ]

                if us100_positions:
                    bid, offer = self.get_rest_market_quote()

                    for item in us100_positions:
                        position = item.get("position", {})
                        deal_id = position.get("dealId")
                        direction = position.get("direction")
                        stop_level = position.get("stopLevel")

                        # Si el listado no incluye stopLevel, consultar el detalle.
                        if stop_level is None and deal_id:
                            detail = self.request(
                                "GET", f"/api/v1/positions/{deal_id}"
                            )
                            position = detail.get("position", {})
                            direction = position.get("direction", direction)
                            stop_level = position.get("stopLevel")

                        if not deal_id or direction not in ("BUY", "SELL"):
                            logging.warning(
                                "No se pudo identificar dealId/dirección "
                                "para una posición %s.", EPIC
                            )
                            continue

                        if stop_level is None:
                            logging.error(
                                "La posición %s no informa stopLevel. "
                                "No se puede activar el cierre de emergencia.",
                                deal_id,
                            )
                            continue

                        stop_level = float(stop_level)
                        hit = (
                            bid <= stop_level
                            if direction == "BUY"
                            else offer >= stop_level
                        )

                        if hit:
                            self.close_position_emergency(
                                deal_id, direction, stop_level
                            )

            except Exception:
                logging.exception(
                    "Error en el vigilante REST de Stop Loss; "
                    "se volverá a comprobar."
                )

            time.sleep(self.stop_monitor_interval)

    def check_take_profit(self, bid, offer):
        if not self.take_profit_position:
            return

        with self.take_profit_lock:
            if self.take_profit_closing:
                return

            trade = self.take_profit_position
            if trade["direction"] == "BUY":
                hit = bid >= trade["target"]
            else:
                hit = offer <= trade["target"]

            if not hit:
                return

            now = time.time()
            if now - self.take_profit_last_attempt < 5:
                return

            self.take_profit_last_attempt = now
            self.take_profit_closing = True

        deal_id = trade["deal_id"]

        # Evita que el TP y el vigilante REST del SL cierren el mismo deal
        # simultáneamente.
        with self.stop_monitor_lock:
            if deal_id in self.emergency_closing_ids:
                with self.take_profit_lock:
                    self.take_profit_closing = False
                return
            self.emergency_closing_ids.add(deal_id)

        try:
            if not self.position_is_open(deal_id):
                logging.info(
                    "La posición %s ya no está abierta; vigilancia TP finalizada.",
                    deal_id,
                )
                self.take_profit_position = None
                return

            logging.warning(
                "TAKE PROFIT ALCANZADO | %s | objetivo=%.2f",
                trade["direction"],
                trade["target"],
            )

            result = self.request(
                "DELETE", f"/api/v1/positions/{deal_id}"
            )
            reference = result.get("dealReference")

            if reference:
                confirmation = self.request(
                    "GET", f"/api/v1/confirms/{reference}"
                )
                logging.info("Confirmación de cierre TP: %s", confirmation)
                if confirmation.get("dealStatus") != "ACCEPTED":
                    logging.error(
                        "El cierre TP no fue confirmado como ACCEPTED. "
                        "Se mantendrá la vigilancia.",
                    )

            if not self.position_is_open(deal_id):
                logging.info("CIERRE TP VERIFICADO | dealId=%s", deal_id)
                self.take_profit_position = None
            else:
                logging.error(
                    "La posición %s sigue abierta tras el intento TP; "
                    "se reintentará con la siguiente cotización.",
                    deal_id,
                )

        except RuntimeError as exc:
            if "error.not-found.dealId" in str(exc):
                try:
                    if not self.position_is_open(deal_id):
                        logging.info(
                            "Capital.com ya no muestra abierta la posición %s.",
                            deal_id,
                        )
                        self.take_profit_position = None
                    else:
                        logging.exception(
                            "Capital.com devolvió 404 pero la posición aún "
                            "aparece abierta; se mantiene vigilancia TP."
                        )
                except Exception:
                    logging.exception(
                        "No se pudo verificar la posición tras el 404 del TP."
                    )
            else:
                logging.exception(
                    "Error al cerrar por Take Profit; se reintentará."
                )

        except Exception:
            logging.exception("Error inesperado en el cierre por Take Profit")

        finally:
            with self.stop_monitor_lock:
                self.emergency_closing_ids.discard(deal_id)
            with self.take_profit_lock:
                self.take_profit_closing = False

    def open_position(self, direction, upper_band=None, lower_band=None):
        logging.info(
            "open_position() llamada: dirección=%s | ENABLE_TRADING=%s",
            direction,
            ENABLE_TRADING,
        )
        if not ENABLE_TRADING:
            logging.info(
                "SIMULACIÓN: señal %s; no se envió ninguna orden",
                direction,
            )
            return

        if self.has_open_position():
            logging.info(
                "Ya existe una posición %s; señal ignorada", EPIC
            )
            return

        payload = {
            "epic": EPIC,
            "direction": direction,
            "size": SIZE,
            "stopDistance": STOP_DISTANCE,
            "profitDistance": PROFIT_DISTANCE,
        }

        logging.info("Enviando orden DEMO: %s", payload)

        result = self.request(
            "POST", "/api/v1/positions", json=payload
        )

        deal_reference = result.get("dealReference")

        if not deal_reference:
            raise RuntimeError(
                "La orden no devolvió dealReference. "
                "Verifica la cuenta antes de volver a intentarlo."
            )

        confirmation = self.request(
            "GET", f"/api/v1/confirms/{deal_reference}"
        )

        logging.info("Confirmación de orden: %s", confirmation)

        if confirmation.get("dealStatus") != "ACCEPTED":
            raise RuntimeError(
                "Orden no confirmada como ACCEPTED; "
                "revisa la cuenta DEMO."
            )

        logging.info("Orden DEMO confirmada: %s", direction)

        affected_deals = confirmation.get("affectedDeals", [])

        deal_id = next(
            (
                deal.get("dealId")
                for deal in affected_deals
                if deal.get("status") == "OPENED"
                and deal.get("dealId")
            ),
            None,
        )

        entry_price = confirmation.get("level")

        if deal_id and entry_price is not None:
            if upper_band is not None and lower_band is not None:
                self.save_entry_bollinger(
                    deal_id,
                    deal_reference,
                    direction,
                    entry_price,
                    upper_band,
                    lower_band,
                )
            else:
                logging.warning(
                    "La operación %s fue abierta sin snapshot completo de Bollinger.",
                    deal_id,
                )

            logging.info(
                "Protección break-even programada "
                "| dealId=%s | entrada=%.2f",
                deal_id,
                float(entry_price),
            )

            threading.Thread(
                target=self.move_stop_to_breakeven,
                args=(deal_id, float(entry_price)),
                daemon=True,
            ).start()

        else:
            logging.error(
                "No se pudo programar break-even. "
                "No se encontró dealId o precio de entrada: %s",
                confirmation,
            )

        if deal_id and entry_price is not None:
            target = (
                float(entry_price) + PROFIT_DISTANCE
                if direction == "BUY"
                else float(entry_price) - PROFIT_DISTANCE
            )

            self.take_profit_position = {
                "deal_id": deal_id,
                "direction": direction,
                "target": target,
            }

            logging.info(
                "Vigilante Take Profit activo | objetivo=%.2f",
                target,
            )

    def execute_signal(self, direction, minute_key, upper_band, lower_band):
        try:
            self.open_position(direction, upper_band, lower_band)
        except Exception:
            logging.exception(
                "Resultado de orden incierto para %s. "
                "No se reenviará automáticamente.", minute_key
            )
        finally:
            with self.order_lock:
                self.order_in_flight = False

    def trigger(self, direction, minute_key, price, band, upper_band, lower_band):
        # Una sola señal por minuto, incluso si llegan muchos ticks.
        with self.order_lock:
            if self.order_in_flight:
                return

            if minute_key == self.last_triggered_minute:
                return

            self.last_triggered_minute = minute_key
            STATE_FILE.write_text(minute_key)

            self.order_in_flight = True

        logging.warning(
            "SEÑAL %s | vela=%s | precio=%.2f | banda=%.2f",
            direction, minute_key, price, band,
        )

        threading.Thread(
            target=self.execute_signal,
            args=(direction, minute_key, upper_band, lower_band),
            daemon=True,
        ).start()

    def process_quote(self, payload):
        if payload.get("epic") != EPIC:
            return

        bid = payload.get("bid")
        offer = payload.get("ofr")

        if bid is None or offer is None:
            return

        self.check_take_profit(float(bid), float(offer))

        price = (float(bid) + float(offer)) / 2

        timestamp = payload.get("timestamp")
        if timestamp is None:
            return

        quote_time = datetime.fromtimestamp(
            float(timestamp) / 1000,
            tz=timezone.utc,
        )

        # Inicio del período de dos minutos.
        period_start = quote_time.replace(
            minute=(quote_time.minute // 2) * 2,
            second=0,
            microsecond=0,
        )

        period_key = period_start.isoformat()

        # Primera cotización después de iniciar o reconectar.
        if self.current_candle is None:
            self.current_candle = {
                "minute": period_start,
                "key": period_key,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "eligible": False,
                "evaluated": False,
            }

            logging.info(
                "Esperando el siguiente período completo de 2 minutos."
            )
            return

        candle = self.current_candle

        # Comenzó un nuevo período de dos minutos.
        if period_start != candle["minute"]:

            # Guardar el cierre del período anterior si fue completo.
            if candle["eligible"]:
                self.closed_closes.append(candle["close"])
                self.closed_closes = self.closed_closes[-19:]

                logging.info(
                    "Vela de 2 minutos cerrada | %s | cierre=%.2f",
                    candle["key"],
                    candle["close"],
                )

            self.current_candle = {
                "minute": period_start,
                "key": period_key,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "eligible": True,
                "evaluated": False,
            }

            candle = self.current_candle

            logging.info(
                "Nuevo período de 2 minutos: %s",
                period_key,
            )

        else:
            # Actualizar la vela sintética actual.
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price

        # No utilizar períodos incompletos después de iniciar
        # o reconectar el WebSocket.
        if not candle["eligible"]:
            return

        # Esperar hasta el segundo 02.
        if quote_time.second < 2:
            return

        # Una sola evaluación por cada período de dos minutos.
        if candle["evaluated"]:
            return

        candle["evaluated"] = True

        middle, upper, lower = self.bands(price)

        if lower is None:
            logging.warning(
                "No hay suficientes datos para calcular Bollinger."
            )
            return

        logging.info(
            "EVALUACIÓN BOLLINGER | %s | segundo=%02d | "
            "precio=%.2f | inferior=%.2f | superior=%.2f",
            period_key,
            quote_time.second,
            price,
            lower,
            upper,
        )

        # BUY: precio por debajo de la banda inferior.
        if price < lower:
            self.trigger(
                "BUY",
                period_key,
                price,
                lower,
                upper,
                lower,
            )

        # SELL: precio por encima de la banda superior.
        elif price > upper:
            self.trigger(
                "SELL",
                period_key,
                price,
                upper,
                upper,
                lower,
            )

        else:
            logging.info(
                "Sin señal: precio dentro de las bandas."
            )

    
    def on_open(self, ws):
        logging.info("WebSocket conectado")

        ws.send(json.dumps({
            "destination": "marketData.subscribe",
            "correlationId": "1",
            "cst": self.cst,
            "securityToken": self.security_token,
            "payload": {"epics": [EPIC]},
        }))

        self.ws_keepalive_stop.clear()

        def keepalive():
            while not self.ws_keepalive_stop.wait(20):
                try:
                    ws.send(json.dumps({
                        "destination": "ping",
                        "correlationId": "keepalive",
                        "cst": self.cst,
                        "securityToken": self.security_token,
                    }))
                except Exception:
                    logging.warning("No se pudo enviar ping WebSocket")
                    return

        threading.Thread(
            target=keepalive, daemon=True
        ).start()

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            destination = data.get("destination")

            if data.get("status") == "ERROR":
                logging.error("Error WebSocket: %s", data)
                return

            if destination == "quote":
                self.process_quote(data.get("payload", {}))

            elif destination == "marketData.subscribe":
                logging.info("Respuesta de suscripción: %s", data)

        except Exception:
            logging.exception("Error procesando mensaje WebSocket")

    def on_error(self, ws, error):
        logging.error("WebSocket: %s", error)

    def on_close(self, ws, status_code, message):
        self.ws_keepalive_stop.set()

        # La vela en curso puede haber perdido cotizaciones.
        if self.current_candle is not None:
            self.current_candle["eligible"] = False

        logging.warning(
            "WebSocket cerrado: %s %s", status_code, message
        )

    def balance_loop(self):
        while True:
            time.sleep(BALANCE_INTERVAL)

            try:
                data = self.request("GET", "/api/v1/accounts")

                for account in data.get("accounts", []):
                    balance = account.get("balance", {})
                    account_name = account.get("accountName", "Cuenta")
                    account_balance = balance.get("balance", "N/D")
                    available = balance.get("available", "N/D")
                    profit_loss = balance.get("profitLoss", "N/D")

                    logging.info(
                        "Saldo %s: %s | Disponible: %s | P/L: %s",
                        account_name,
                        account_balance,
                        available,
                        profit_loss,
                    )

                    # Guardar cada lectura al final del archivo, sin borrar
                    # los registros anteriores. La hora usa la zona local
                    # configurada en el equipo donde se ejecuta el bot.
                    timestamp = datetime.now().astimezone().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    try:
                        file_is_new = (
                            not BALANCE_LOG_FILE.exists()
                            or BALANCE_LOG_FILE.stat().st_size == 0
                        )
                        with BALANCE_LOG_FILE.open("a", encoding="utf-8") as log_file:
                            if file_is_new:
                                log_file.write(
                                    "Fecha y hora | Cuenta | Account | Available | P/L\n"
                                )
                            log_file.write(
                                f"{timestamp} | {account_name} | "
                                f"{account_balance} | {available} | {profit_loss}\n"
                            )
                    except OSError:
                        logging.exception(
                            "No se pudo guardar el historial de saldo en %s",
                            BALANCE_LOG_FILE,
                        )

            except Exception:
                logging.exception("No se pudo actualizar el saldo")

    def run_forever(self):
        self.seed_history()

        logging.info("Bot iniciado para %s", EPIC)
        logging.info("Trading DEMO habilitado: %s", ENABLE_TRADING)
        logging.info("Órdenes desactivadas por defecto")

        threading.Thread(
            target=self.balance_loop, daemon=True
        ).start()

        # Este hilo no depende del WebSocket y recupera automáticamente
        # las posiciones abiertas que ya existan al iniciar el bot.
        threading.Thread(
            target=self.stop_loss_monitor_loop,
            daemon=True,
            name="stop-loss-rest-monitor",
        ).start()

        while True:
            try:
                self.ws_app = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )

                self.ws_app.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                )

            except KeyboardInterrupt:
                logging.info("Bot detenido por el usuario")
                break

            except Exception:
                logging.exception("Fallo en conexión WebSocket")

            self.ws_keepalive_stop.set()

            # Renovar la sesión antes de reconectar.
            time.sleep(5)

            try:
                self.login()
            except Exception:
                logging.exception("No se pudo renovar la sesión")
                time.sleep(10)


if __name__ == "__main__":
    try:
        CapitalBot().run_forever()
    except KeyboardInterrupt:
        logging.info("Bot detenido")
    except Exception:
        logging.exception("No se pudo iniciar el bot")

