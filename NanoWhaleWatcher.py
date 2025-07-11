from flask import Flask, render_template, request, url_for
from flask_sqlalchemy import SQLAlchemy
import threading
import websocket
import json
import time
from datetime import datetime, timedelta, timezone
from extensions import db
import os
from dotenv import load_dotenv
import logging
import requests
import concurrent.futures

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///transactions.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db.init_app(app)

from models import Transaction

# Constants
MINIMUM_DETECTABLE_AMOUNT = 1000  # 1000 XNO in raw
RAW_CONVERSION = 10**30
MINIMUM_DETECTABLE_AMOUNT_RAW = MINIMUM_DETECTABLE_AMOUNT * RAW_CONVERSION
PER_PAGE = 20
MAX_RETRIES = 5
RETRY_DELAY = 30
PING_INTERVAL = 30  # Send ping every 30 seconds
CONNECTION_TIMEOUT = 120  # Consider connection dead if no message for 2 minutes


class AliasManager:
    def __init__(self):
        self.aliases = {}
        self.update_thread = threading.Thread(target=self.fetch_aliases)
        self.update_thread.daemon = True
        self.update_thread.start()

    def fetch_aliases(self):
        while True:
            try:
                response = requests.get(
                    "https://nanobrowse.com/api/search/known_accounts"
                )
                if response.status_code == 200:
                    aliases = response.json()
                    self.aliases = {item["account"]: item["name"] for item in aliases}
            except Exception as e:
                logging.error(f"Failed to fetch aliases: {e}")
            time.sleep(86400)

    def get_alias(self, account):
        return self.aliases.get(account, account)

    @staticmethod
    def trim_account(account):
        if account and len(account) > 30:
            return f"{account[:10]}...{account[-6:]}"
        return account


alias_manager = AliasManager()


class WebSocketManager:
    def __init__(self):
        urls_env = os.getenv("WEBSOCKET_URLS", "")
        self.urls = [url.strip() for url in urls_env.split(",") if url.strip()]
        self.connections = {}
        self.lock = threading.Lock()
        self.last_message_time = None
        self.shutdown_flag = False
        
        # Fixed thread pool - no dynamic resizing
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.urls))
        
        # Start health check thread
        self.health_thread = threading.Thread(target=self.health_check)
        self.health_thread.daemon = True
        self.health_thread.start()

    def start_connections(self):
        for url in self.urls:
            websocket_url = (
                f"wss://{url}"
                if not url.startswith(("localhost", "[::1]"))
                else f"ws://{url}"
            )
            self.connections[url] = {
                "ws": None,
                "connected": False,
                "retry_count": 0,
                "last_ping": None,
                "last_message": None
            }
            self.executor.submit(self.connect_with_retry, url, websocket_url)

    def connect_with_retry(self, url, websocket_url):
        while not self.shutdown_flag and self.connections[url]["retry_count"] < MAX_RETRIES:
            try:
                logging.info(f"Connecting to {url}")
                ws = websocket.WebSocketApp(
                    websocket_url,
                    on_open=lambda ws: self.on_open(ws, url),
                    on_message=lambda ws, message: self.on_message(ws, message, url),
                    on_error=lambda ws, error: self.on_error(ws, error, url),
                    on_close=lambda ws, close_status_code, close_msg: self.on_close(
                        ws, url, close_status_code, close_msg
                    ),
                    on_ping=lambda ws, message: self.on_ping(ws, message, url),
                    on_pong=lambda ws, message: self.on_pong(ws, message, url)
                )
                
                with self.lock:
                    self.connections[url]["ws"] = ws
                
                ws.run_forever(ping_interval=PING_INTERVAL)
                
            except Exception as e:
                logging.error(f"WebSocket connection failed for {url}: {e}")
                with self.lock:
                    self.connections[url]["connected"] = False
                    self.connections[url]["retry_count"] += 1
                
                if not self.shutdown_flag:
                    time.sleep(RETRY_DELAY)

    def on_open(self, ws, url):
        with self.lock:
            self.connections[url]["connected"] = True
            self.connections[url]["retry_count"] = 0
            self.connections[url]["last_ping"] = datetime.now(timezone.utc)
            self.connections[url]["last_message"] = datetime.now(timezone.utc)
        
        # Subscribe to necessary topics
        subscribe_message = {
            "action": "subscribe",
            "topic": "confirmation",
            "options": {"confirmation_type": "all"},
        }
        ws.send(json.dumps(subscribe_message))
        logging.info(f"WebSocket connected to {url}")

    def on_message(self, ws, message, url):
        with app.app_context():
            current_time = datetime.now(timezone.utc)
            self.last_message_time = current_time
            
            with self.lock:
                self.connections[url]["last_message"] = current_time
            
            try:
                data = json.loads(message)
                if data.get("topic") == "confirmation":
                    transaction_data = data["message"]
                    transaction_time = datetime.fromtimestamp(
                        int(data["time"]) / 1000, timezone.utc
                    )

                    # Check if the subtype is 'send' and the amount is greater than the limit
                    if transaction_data["block"]["subtype"] == "send":
                        if (
                            float(transaction_data.get("amount", 0))
                            > MINIMUM_DETECTABLE_AMOUNT_RAW
                        ):
                            sender = transaction_data["account"]
                            receiver = transaction_data["block"].get("link_as_account")
                            if sender != receiver:  # Ignore self transactions
                                existing_transaction = Transaction.query.filter_by(
                                    hash=transaction_data["hash"]
                                ).first()
                                if not existing_transaction:
                                    transaction = Transaction(
                                        sender=sender,
                                        receiver=receiver,
                                        amount=float(
                                            format(
                                                float(transaction_data["amount"])
                                                / RAW_CONVERSION,
                                                ".2f",
                                            )
                                        ),
                                        time=transaction_time,
                                        hash=transaction_data["hash"],
                                    )
                                    db.session.add(transaction)
                                    db.session.commit()
            except Exception as e:
                logging.error(f"Error processing message from {url}: {e}")

    def on_ping(self, ws, message, url):
        with self.lock:
            self.connections[url]["last_ping"] = datetime.now(timezone.utc)

    def on_pong(self, ws, message, url):
        with self.lock:
            self.connections[url]["last_ping"] = datetime.now(timezone.utc)

    def on_error(self, ws, error, url):
        logging.error(f"WebSocket error for {url}: {error}")
        with self.lock:
            self.connections[url]["connected"] = False

    def on_close(self, ws, url, close_status_code, close_msg):
        logging.info(f"WebSocket closed for {url} with code {close_status_code}: {close_msg}")
        with self.lock:
            self.connections[url]["connected"] = False
        
        # Reconnection will be handled by the health check

    def health_check(self):
        """Check connection health and restart dead connections"""
        while not self.shutdown_flag:
            try:
                current_time = datetime.now(timezone.utc)
                
                for url, conn in self.connections.items():
                    with self.lock:
                        if conn["connected"] and conn["last_message"]:
                            time_since_message = (current_time - conn["last_message"]).total_seconds()
                            
                            # If no message for CONNECTION_TIMEOUT seconds, consider connection dead
                            if time_since_message > CONNECTION_TIMEOUT:
                                logging.warning(f"Connection to {url} appears dead (no messages for {time_since_message}s), restarting...")
                                conn["connected"] = False
                                if conn["ws"]:
                                    try:
                                        conn["ws"].close()
                                    except:
                                        pass
                                
                                # Restart connection
                                websocket_url = (
                                    f"wss://{url}"
                                    if not url.startswith(("localhost", "[::1]"))
                                    else f"ws://{url}"
                                )
                                self.executor.submit(self.connect_with_retry, url, websocket_url)
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logging.error(f"Error in health check: {e}")
                time.sleep(30)

    def shutdown(self):
        self.shutdown_flag = True
        with self.lock:
            for url, conn in self.connections.items():
                if conn["ws"]:
                    try:
                        conn["ws"].close()
                    except:
                        pass
        self.executor.shutdown(wait=True)


ws_manager = WebSocketManager()


@app.route("/websocket-status")
def websocket_status():
    try:
        # Check if any connection is currently open
        any_connected = any(
            conn["connected"] for conn in ws_manager.connections.values()
        )
        if any_connected and ws_manager.last_message_time:
            last_message_str = ws_manager.last_message_time.strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )
            last_message_ago = int(
                (datetime.now(timezone.utc) - ws_manager.last_message_time).total_seconds()
            )
            status = {
                "status": True,
                "last_message": last_message_str,
                "last_message_ago": last_message_ago,
            }
        else:
            status = {
                "status": False,
                "last_message": (
                    "No message received" if any_connected else "All connections closed"
                ),
            }
        return status
    except Exception as e:
        logging.error(f"Error in websocket-status: {e}")
        return {"status": False, "last_message": "Error fetching status"}


@app.route("/", methods=["GET"])
def index():
    page = request.args.get("page", 1, type=int)
    filtered = False
    date_range = None
    time_frame_display = None

    # Default values for filters
    min_amount = request.args.get(
        "min_amount", default=MINIMUM_DETECTABLE_AMOUNT, type=int
    )
    if min_amount > 100000000000000000:
        min_amount = 100000000000000000
    time_frame = request.args.get("time_frame", default="30d")

    # Calculate the start and end time based on the time frame
    end_time = datetime.now()
    if time_frame == "24h":
        start_time = end_time - timedelta(days=1)
        date_range = "Last 24 Hours"
    elif time_frame == "7d":
        start_time = end_time - timedelta(days=7)
        date_range = "Last 7 Days"
    elif time_frame == "30d":
        start_time = end_time - timedelta(days=30)
        date_range = "Last 30 Days"
    else:
        # Custom date range
        start_date_str = request.args.get("start_date")
        end_date_str = request.args.get("end_date")
        if start_date_str and end_date_str:
            start_time = datetime.strptime(start_date_str, "%Y-%m-%d")
            end_time = datetime.strptime(end_date_str, "%Y-%m-%d")
            date_range = (
                f"{start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}"
            )
            time_frame_display = "Custom Date Range"
        else:
            # Default to the last 30 days if no custom date range is provided
            start_time = end_time - timedelta(days=30)
            time_frame = "30d"
            time_frame_display = "Last 30 Days"

    filtered = True

    # Filtering and pagination logic
    transactions_query = Transaction.query.filter(
        Transaction.time >= start_time,
        Transaction.time <= end_time,
        Transaction.amount > min_amount,
    ).order_by(Transaction.time.desc())
    # Pagination URL Generation with Custom Date Range Support
    url_kwargs = {
        "min_amount": min_amount,
        "time_frame": time_frame,
        "start_date": request.args.get("start_date"),
        "end_date": request.args.get("end_date"),
    }
    transactions_paginated = transactions_query.paginate(
        page=page, per_page=PER_PAGE, error_out=False
    )

    if transactions_paginated.has_next:
        url_kwargs["page"] = transactions_paginated.next_num
        next_url = url_for("index", **url_kwargs)
    else:
        next_url = None

    if transactions_paginated.has_prev:
        url_kwargs["page"] = transactions_paginated.prev_num
        prev_url = url_for("index", **url_kwargs)
    else:
        prev_url = None

    transactions_with_alias = []
    for transaction in transactions_paginated.items:
        sender_alias = alias_manager.get_alias(transaction.sender)
        receiver_alias = alias_manager.get_alias(transaction.receiver)
        transactions_with_alias.append(
            {
                "time": transaction.time.isoformat(),
                "sender": sender_alias,
                "receiver": receiver_alias,
                "amount": transaction.amount,
                "hash": transaction.hash,
            }
        )

    return render_template(
        "index.html",
        time_frame=time_frame,
        transactions=transactions_with_alias,
        filtered=filtered,
        min_amount=min_amount,
        date_range=date_range.lower(),
        time_frame_display=time_frame_display,
        MINIMUM_DETECTABLE_AMOUNT=MINIMUM_DETECTABLE_AMOUNT,
        next_url=next_url,
        prev_url=prev_url,
        trim_account=AliasManager.trim_account,
    )


def create_tables():
    with app.app_context():
        db.create_all()


if __name__ == "__main__":
    with app.app_context():
        create_tables()  # Create tables before starting the application
    ws_manager.start_connections()
    try:
        app.run(host=os.getenv("HOST"), port=os.getenv("PORT"))
    finally:
        ws_manager.shutdown()