# -*- coding: utf-8 -*-

"""
MQTT Topic Liveness Monitor
A desktop tool that watches an MQTT broker and reports which topics are
alive, stale, or dead, ranked by criticality (tier). Listens only - never
publishes to the live broker.

Built on: Tkinter UI + paho-mqtt for broker connectivity.
"""

import json
import os
import sys
import threading
import time
import tkinter as tk
from tkinter import ttk
import tkinter.messagebox
import tkinter.filedialog
import queue
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
PROFILES_FILE = BASE_DIR / "profiles.json"
TOPICS_FILE = BASE_DIR / "topics.json"
LOG_FILE = BASE_DIR / "mqtt_monitor.log"

DEFAULT_PROFILES = [
    {
        "name": "Local MQTT broker",
        "address": "localhost",
        "port": 1883,
        "user": "",
        "password": "",
        "client_id_prefix": "liveness-monitor",
        "client_id_suffix": "",
    }
]

DEFAULT_TOPICS = [
    {
        "topic": "cmd/device/fan",
        "class": "device",
        "tier": 1,
        "short_name": "Fan cmd",
        "purpose": "Start/stop and speed commands for plant fans",
        "if_silent": "Alert immediately; fan may be down",
        "expected_interval_sec": 60,
        "measured_interval_sec": 60.0,
        "last_message_at": None,
        "last_message_payload": None,
    },
    {
        "topic": "cmd/device/damper",
        "class": "device",
        "tier": 1,
        "short_name": "Damper cmd",
        "purpose": "Position commands for plant dampers",
        "if_silent": "Alert immediately; damper may be stuck",
        "expected_interval_sec": 60,
        "measured_interval_sec": 60.0,
        "last_message_at": None,
        "last_message_payload": None,
    },
    {
        "topic": "state/device/fan/speed",
        "class": "sensor",
        "tier": 2,
        "short_name": "Fan speed state",
        "purpose": "Live fan speed feedback from controllers",
        "if_silent": "Alert after a short window",
        "expected_interval_sec": 10,
        "measured_interval_sec": 10.0,
        "last_message_at": None,
        "last_message_payload": None,
    },
    {
        "topic": "state/device/temp",
        "class": "sensor",
        "tier": 2,
        "short_name": "Temp state",
        "purpose": "Temperature readings from plant sensors",
        "if_silent": "Alert after a short window",
        "expected_interval_sec": 30,
        "measured_interval_sec": 30.0,
        "last_message_at": None,
        "last_message_payload": None,
    },
    {
        "topic": "heartbeat/controller/main",
        "class": "control",
        "tier": 3,
        "short_name": "Controller heartbeat",
        "purpose": "Liveness heartbeat from the main controller",
        "if_silent": "Warn after stale threshold",
        "expected_interval_sec": 5,
        "measured_interval_sec": 5.0,
        "last_message_at": None,
        "last_message_payload": None,
    },
]

# Tier meanings (ranked by criticality)
TIER_NAMES = {1: "Critical", 2: "Warning", 3: "Info"}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")

def log_message(level, msg):
    """Append a timestamped line to the log file (thread-safe)."""
    line = f"[{now_iso()}] {level} {msg}\n"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass
    try:
        sys.stdout.write(line)
        sys.stdout.flush()
    except Exception:
        pass

def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            return default
    except (OSError, ValueError):
        return default

def save_json(path, data):
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)
        return True
    except OSError:
        return False

def human_delta(seconds):
    if seconds is None or seconds < 0:
        return "never"
    s = max(0.0, seconds)
    if s < 60:
        return f"{s:,.1f}s"
    if s < 3600:
        return f"{s/60:,.1f}m"
    return f"{s/3600:,.1f}h"

def state_label(state):
    return {0: "DEAD", 1: "STALE", 2: "ALIVE"}.get(state, "UNKNOWN")


def classify_topic(topic):
    """Best-effort classification of a topic by its first segment."""
    head = topic.split("/", 1)[0].lower()
    if head.startswith("cmd"):
        return "device"
    if head.startswith("state") or head.startswith("sensor"):
        return "sensor"
    if head.startswith("heartbeat"):
        return "control"
    if head in ("alarm", "alerts", "event", "events"):
        return "alarm"
    if head in ("tele", "telemetry"):
        return "telemetry"
    return "other"


def guess_tier(cls):
    """Tier (criticality) guess for a discovered topic class."""
    return {"device": 1, "sensor": 2, "control": 3, "alarm": 1}.get(cls, 2)

# ---------------------------------------------------------------------------
# Profile management
# ---------------------------------------------------------------------------
def load_profiles():
    return load_json(PROFILES_FILE, DEFAULT_PROFILES)

def save_profiles(profiles):
    return save_json(PROFILES_FILE, profiles)

def get_default_profile():
    p = load_json(PROFILES_FILE, DEFAULT_PROFILES)
    return p[0] if p else DEFAULT_PROFILES[0]

# ---------------------------------------------------------------------------
# MQTT state machine
# ---------------------------------------------------------------------------
class MQTTStateMachine:
    """Tracks the liveness of a single topic.
    States: 0=DEAD, 1=STALE, 2=ALIVE.
    """
    DEAD = 0
    STALE = 1
    ALIVE = 2

    def __init__(self, topic, expected_interval_sec, stale_threshold_multiplier=2.0):
        self.topic = topic
        self.expected_interval_sec = expected_interval_sec
        self.stale_threshold_sec = expected_interval_sec * stale_threshold_multiplier
        self._history = []
        self._last_state = None
        self.state = self.DEAD
        self.last_message_at = None
        self.last_message_payload = None

    def add_message(self, payload, at):
        self._history.append({"payload": payload, "at": at})
        if len(self._history) > 20:
            self._history = self._history[-20:]
        self.last_message_at = at
        self.last_message_payload = payload
        self._recompute_state()

    def _recompute_state(self):
        now = time.time()
        if self.last_message_at is None:
            self.state = self.DEAD
            return
        age = now - self.last_message_at
        if age <= self.expected_interval_sec:
            self.state = self.ALIVE
        elif age <= self.stale_threshold_sec:
            self.state = self.STALE
        else:
            self.state = self.DEAD

    def measured_interval_sec(self):
        """Latest measured interval between consecutive messages.
        """
        if len(self._history) < 2:
            return self.expected_interval_sec
        prev = self._history[-2]
        return max(0.0, self.last_message_at - prev["at"])

    def state_changed(self, previous):
        return self.state != previous

    def serialize(self):
        return {
            "topic": self.topic,
            "expected_interval_sec": self.expected_interval_sec,
            "stale_threshold_sec": self.stale_threshold_sec,
            "state": self.state,
            "state_label": state_label(self.state),
            "age": time.time() - self.last_message_at if self.last_message_at else None,
            "measured_interval_sec": self.measured_interval_sec(),
            "last_message_at": self.last_message_at,
            "last_message_payload": self.last_message_payload,
        }

# ---------------------------------------------------------------------------
# MQTT client (listens only, auto-reconnect)
# ---------------------------------------------------------------------------
class MQTTClient:
    """Single shared paho-mqtt client. Listens only; never publishes."""
    
    def __init__(self, on_message, on_state_changed):
        self.on_message = on_message
        self.on_state_changed = on_state_changed
        self._lock = threading.Lock()
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_subscribe = self._on_subscribe
        self._connected = threading.Event()
        self._reconnect_delay = 1
    
    def set_auth(self, user, password):
        self._client.username_pw_set(user, password)
    
    def connect(self, address, port):
        try:
            self._client.connect(address, port, keepalive=60)
            self._reconnect_delay = 1
            self._client.loop_start()
            self._connected.wait(timeout=10)
            if not self._connected.is_set():
                raise RuntimeError("connect timed out")
        except Exception as e:
            log_message("ERROR", f"connect {address}:{port} -> {e}")
            raise
    
    def disconnect(self):
        try:
            self._client.disconnect()
        finally:
            self._client.loop_stop()
            self._connected.clear()
    
    def subscribe(self, topic, qos=0):
        self._client.subscribe(topic, qos)

    def unsubscribe(self, topic):
        self._client.unsubscribe(topic)
    
    def publish(self, topic, payload=None, qos=0, retain=False):
        # Intentionally NOT implemented for this tool - listens only.
        raise RuntimeError("publish() is not supported; this tool listens only")
    
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected.set()
            log_message("INFO", "connected to broker")
            self._reconnect_delay = 1
        else:
            log_message("ERROR", f"connect returned rc={rc}")
            self._connected.clear()
    
    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        if rc != 0:
            log_message("WARN", "disconnected, auto-reconnect scheduled")
    
    def _on_message(self, client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        self.on_message(msg.topic, payload)
    
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        pass
    
    def reconnect(self):
        # Auto-reconnect with backoff.
        for attempt in range(10):
            try:
                self._client.reconnect()
                self._connected.wait(timeout=10)
                if self._connected.is_set():
                    log_message("INFO", "reconnected to broker")
                    return
            except Exception as e:
                log_message("WARN", f"reconnect attempt {attempt+1} failed: {e}")
                time.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30)
        log_message("ERROR", "cannot reconnect to broker")

# ---------------------------------------------------------------------------
# Topic library (load/save JSON)
# ---------------------------------------------------------------------------
def topic_from_dict(d):
    return {
        "topic": d.get("topic", ""),
        "class": d.get("class", ""),
        "tier": int(d.get("tier", 2)),
        "short_name": d.get("short_name", ""),
        "purpose": d.get("purpose", ""),
        "if_silent": d.get("if_silent", ""),
        "expected_interval_sec": float(d.get("expected_interval_sec", 60)),
        "measured_interval_sec": float(d.get("measured_interval_sec", 60)),
        "last_message_at": d.get("last_message_at"),
        "last_message_payload": d.get("last_message_payload"),
    }

def topic_to_dict(t):
    if isinstance(t, dict):
        return {k: t.get(k) for k in (
            "topic", "class", "tier", "short_name", "purpose", "if_silent",
            "expected_interval_sec", "measured_interval_sec",
            "last_message_at", "last_message_payload")}
    return {
        "topic": t.topic,
        "class": t.topic_class,
        "tier": t.tier,
        "short_name": t.short_name,
        "purpose": t.purpose,
        "if_silent": t.if_silent,
        "expected_interval_sec": t.expected_interval_sec,
        "measured_interval_sec": t.measured_interval_sec,
        "last_message_at": t.last_message_at,
        "last_message_payload": t.last_message_payload,
    }

def load_topics():
    data = load_json(TOPICS_FILE, DEFAULT_TOPICS)
    return [topic_from_dict(d) for d in data]

def save_topics(topics):
    return save_json(TOPICS_FILE, [topic_to_dict(t) for t in topics])


# ---------------------------------------------------------------------------
# Main application (Tkinter UI)
# ---------------------------------------------------------------------------
class App(tk.Tk):
    """Main application window."""

    def __init__(self):
        super().__init__()
        self.title("MQTT Topic Liveness Monitor")
        self.geometry("1400x850")
        self.minsize(1000, 700)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

        # State
        self.profiles = load_profiles()
        self.topics = load_topics()
        self.state_machines = {t["topic"]: MQTTStateMachine(t["topic"], t["expected_interval_sec"]) for t in self.topics}
        self.topic_details = {t['topic']: t for t in self.topics}
        self.current_profile = self.profiles[0] if self.profiles else None
        self._last_logged_state = {}
        self.ui_queue = queue.Queue()
        self.sm_lock = threading.Lock()
        self.selected_topic = None
        self.discovery_active = False
        self.discovered = {}
        self.connected = False
        self.running = True
        self.terminal_filter = ""
        self.terminal_changes_only = False
        self.terminal_paused = False
        self.terminal_lock = threading.Lock()
        self.terminal_lines = []
        self.history_lines = []
        self.msg_count = 0
        self.last_msg_rate = 0.0
        self.msg_rate_start = time.time()
        self.msg_rate_count = 0

        # Setup UI
        self.setup_menubar()
        self.setup_connection_bar()
        self.setup_notebook()
        self.setup_table()
        self.setup_terminal()
        self.setup_history()
        self.setup_settings()
        self.setup_discovery()
        self.update_idletasks()
        self.refresh_topic_table()
        self.refresh_terminal()
        self.refresh_history()
        self.refresh_connection_bar()

        # Main-thread UI tick (drains MQTT queue, decays states, refreshes widgets)
        self.after(500, self.ui_tick)

    def on_close(self):
        self.running = False
        self.destroy()

    def setup_menubar(self):
        menubar = tk.Menu(self)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Exit", command=self.on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.config(menu=menubar)

    def show_about(self):
        msg = "MQTT Topic Liveness Monitor\n\nA desktop tool that watches an MQTT broker and reports which topics are alive, stale, or dead."
        tk.messagebox.showinfo("About", msg)

    def setup_connection_bar(self):
        bar = tk.Frame(self, bd=1, relief=tk.SUNKEN)
        bar.pack(side=tk.TOP, fill=tk.X)
        tk.Label(bar, text="Connection").pack(side=tk.LEFT, padx=5, pady=3)
        self.profile_var = tk.StringVar(value=self.profile_names()[0] if self.profile_names() else "None")
        self.profile_menu = tk.OptionMenu(bar, self.profile_var, *self.profile_names(), command=self.on_profile_changed)
        self.profile_menu.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
        tk.Button(bar, text="Connect", command=self.toggle_connect).pack(side=tk.LEFT, padx=5)
        self.broker_state_label = tk.Label(bar, text="Disconnected", fg="red")
        self.broker_state_label.pack(side=tk.LEFT, padx=5)
        self.laptop_ip_label = tk.Label(bar, text="IP: --")
        self.laptop_ip_label.pack(side=tk.LEFT, padx=5)
        self.msg_rate_label = tk.Label(bar, text="msgs/s: 0.0")
        self.msg_rate_label.pack(side=tk.RIGHT, padx=5)

    def profile_names(self):
        return [p["name"] for p in self.profiles]

    def on_profile_changed(self, name):
        profile = next((p for p in self.profiles if p["name"] == name), None)
        if profile:
            self.current_profile = profile
            self.load_profile_config(profile)
            self.refresh_connection_bar()

    def load_profile_config(self, profile):
        pass

    def toggle_connect(self):
        if self.connected:
            self.disconnect()
        else:
            self.connect()

    def connect(self):
        profile = self.current_profile
        if not profile:
            tk.messagebox.showerror("Error", "No profile selected")
            return
        try:
            self.mqtt_client = MQTTClient(self.on_mqtt_message, self.on_state_changed)
            if profile.get("user"):
                self.mqtt_client.set_auth(profile["user"], profile.get("password", ""))
            self.mqtt_client.connect(profile["address"], int(profile.get("port", 1883)))
            self.connected = True
            self.broker_state_label.config(text="Connected", fg="green")
            self.refresh_connection_bar()
            self.publish_subscriptions()
        except Exception as e:
            self.broker_state_label.config(text=f"Error: {e}", fg="red")
            self.connected = False
            self.refresh_connection_bar()

    def disconnect(self):
        if hasattr(self, "mqtt_client"):
            self.mqtt_client.disconnect()
        self.connected = False
        self.broker_state_label.config(text="Disconnected", fg="red")
        self.refresh_connection_bar()

    def publish_subscriptions(self):
        if not self.connected:
            return
        for t in self.topics:
            try:
                self.mqtt_client.subscribe(t["topic"])
            except Exception as e:
                log_message("WARN", f"subscribe {t["topic"]}: {e}")
        if self.discovery_active:
            try:
                self.mqtt_client.subscribe("#")
            except Exception as e:
                log_message("WARN", f"subscribe #: {e}")

    def on_mqtt_message(self, topic, payload):
        """Called from the paho network thread - never touch Tk widgets here."""
        now = time.time()
        self.msg_count += 1
        self.msg_rate_count += 1
        if now - self.msg_rate_start >= 1.0:
            self.last_msg_rate = self.msg_rate_count / (now - self.msg_rate_start)
            self.msg_rate_start = now
            self.msg_rate_count = 0
        sm = self.state_machines.get(topic)
        if sm is None:
            # Unknown topic - only relevant while discovering
            self.ui_queue.put(("unknown", topic, payload))
            return
        with self.sm_lock:
            sm.add_message(payload, now)
        self.ui_queue.put(("message", topic, payload))

    def on_state_changed(self, topic, new_state):
        """Called from the paho network thread - never touch Tk widgets here."""
        self.ui_queue.put(("state", topic, new_state))


    def ui_tick(self):
        """Main-thread tick: drain the MQTT queue, decay states, refresh widgets."""
        if not self.running:
            return
        dirty = False
        disc_dirty = False
        while True:
            try:
                ev = self.ui_queue.get_nowait()
            except queue.Empty:
                break
            kind, a, b = ev
            if kind == "message":
                topic, payload = a, b
                sm = self.state_machines.get(topic)
                if sm is None:
                    continue
                prev = self._last_logged_state.get(topic, MQTTStateMachine.DEAD)
                self.log_state_change(topic, payload, sm.state)
                self.add_terminal_line(topic, payload, sm.state,
                                       changed=(prev != sm.state))
                if self.discovery_active:
                    self._record_discovery(topic, payload)
                    disc_dirty = True
            elif kind == "unknown":
                if self.discovery_active:
                    self._record_discovery(a, b)
                    disc_dirty = True
            else:  # "state"
                self.log_state_change(a, None, b)
            dirty = True
        # Time-based decay: ALIVE -> STALE -> DEAD as silence grows
        for topic, sm in self.state_machines.items():
            old = sm.state
            with self.sm_lock:
                sm._recompute_state()
            if sm.state != old:
                dirty = True
                self.log_state_change(topic, None, sm.state)
                self.add_terminal_line(topic, None, sm.state, changed=True)
        if dirty:
            self.refresh_topic_table()
            self.refresh_terminal()
            self.refresh_history()
        if disc_dirty:
            self.refresh_discovery_tree()
        if self.selected_topic:
            self.refresh_detail()
        self.refresh_connection_bar()
        self.refresh_table_stats()
        self.after(500, self.ui_tick)

    def setup_notebook(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.tab_monitor = tk.Frame(self.notebook)
        self.tab_history = tk.Frame(self.notebook)
        self.tab_settings = tk.Frame(self.notebook)
        self.notebook.add(self.tab_monitor, text="Monitor")
        self.notebook.add(self.tab_history, text="History")
        self.notebook.add(self.tab_settings, text="Settings")

    def setup_table(self):
        left = tk.Frame(self.tab_monitor)
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        frame = tk.Frame(left)
        frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        columns = ("topic", "short_name", "class", "tier", "state", "age", "measured", "expected", "activity")
        self.tree = ttk.Treeview(frame, columns=columns, show="headings")
        headings = {
            "topic": "Topic",
            "short_name": "Short Name",
            "class": "Class",
            "tier": "Tier",
            "state": "State",
            "age": "Age",
            "measured": "Measured/Expected",
            "expected": "Expected",
            "activity": "Activity (last 60s)"
        }
        for col, text in headings.items():
            self.tree.heading(col, text=text)
            self.tree.column(col, width=120, anchor=tk.W)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.bind("<<TreeviewSelect>>", self.on_table_select)

        # Detail side panel (below the table)
        detail = tk.Frame(left)
        detail.pack(side=tk.BOTTOM, fill=tk.X, pady=(5, 0))
        tk.Label(detail, text="Topic Detail").pack(side=tk.TOP, anchor=tk.W)
        detail_scroll = ttk.Scrollbar(detail, orient=tk.VERTICAL)
        detail_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.detail_text = tk.Text(detail, height=13, wrap=tk.WORD,
                                   state=tk.DISABLED,
                                   yscrollcommand=detail_scroll.set)
        self.detail_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        detail_scroll.config(command=self.detail_text.yview)

    def refresh_detail(self):
        topic = self.selected_topic
        if not topic or topic not in self.state_machines:
            text = ("Select a topic row to inspect purpose, "
                    "interval stats, and recent messages.")
        else:
            sm = self.state_machines[topic]
            d = self.topic_details.get(topic, {})
            now = time.time()
            age = (now - sm.last_message_at) if sm.last_message_at else None
            ats = [h["at"] for h in sm._history]
            gaps = [b - a for a, b in zip(ats, ats[1:])]
            if gaps:
                n_gaps = len(gaps)
                stats = (f"min {min(gaps):.1f}s / avg {sum(gaps)/n_gaps:.1f}s"
                         f" / max {max(gaps):.1f}s")
            else:
                n_gaps = 0
                stats = "not enough messages yet"
            tier = d.get("tier", 2)
            lines = [
                f"Topic: {topic}",
                (f"Name: {d.get('short_name', '')}   "
                 f"Class: {d.get('class', '')}   "
                 f"Tier: {tier} ({TIER_NAMES.get(tier, '?')})"),
                (f"State: {state_label(sm.state)}   "
                 f"Age: {human_delta(age) if age is not None else 'never'}"),
                f"Purpose: {d.get('purpose', '')}",
                f"If silent: {d.get('if_silent', '')}",
                (f"Expected interval: {d.get('expected_interval_sec', 60):.0f}s"
                 f"   Measured: {sm.measured_interval_sec():.1f}s"),
                f"Interval stats (last {n_gaps}): {stats}",
                "",
                "Last messages:",
            ]
            if sm._history:
                for h in reversed(sm._history):
                    lines.append(
                        f"  {human_delta(now - h['at'])} ago  {h['payload']}")
            else:
                lines.append("  (none yet)")
            text = "\n".join(lines)
        self.detail_text.config(state=tk.NORMAL)
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert("1.0", text)
        self.detail_text.config(state=tk.DISABLED)

    def setup_terminal(self):
        frame = tk.Frame(self.tab_monitor)
        frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        ctrl = tk.Frame(frame)
        ctrl.pack(side=tk.TOP, fill=tk.X)
        tk.Button(ctrl, text="Filter", command=self.toggle_filter).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(ctrl, text="Changes only", command=self.toggle_changes).pack(side=tk.LEFT, padx=2, pady=2)
        tk.Button(ctrl, text="Pause", command=self.toggle_pause).pack(side=tk.LEFT, padx=2, pady=2)
        self.filter_entry = tk.Entry(ctrl, width=20)
        self.filter_entry.pack(side=tk.LEFT, padx=2, pady=2, fill=tk.X, expand=True)
        self.filter_entry.bind("<KeyRelease>", lambda e: self.on_filter_change())
        term = tk.Text(frame, height=15, wrap=tk.WORD)
        term.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)
        self.terminal_text = term

    def on_table_select(self, event):
        selection = self.tree.selection()
        if selection:
            item = self.tree.item(selection[0])
            topic = str(item["values"][0])
            self.selected_topic = topic
            self.filter_entry.delete(0, tk.END)
            self.filter_entry.insert(0, topic)
            self.on_filter_change()
            self.refresh_detail()

    def refresh_terminal(self):
        if self.terminal_paused:
            return
        with self.terminal_lock:
            lines = list(self.terminal_lines)
        f = self.terminal_filter
        if f.startswith("topic:"):
            want = f[6:]
            lines = [l for l in lines if l["topic"] == want]
        elif f:
            fl = f.lower()
            lines = [l for l in lines if fl in l["text"].lower()]
        if self.terminal_changes_only:
            lines = [l for l in lines if l.get("changed")]
        self.terminal_text.delete(1.0, tk.END)
        for line in reversed(lines):
            self.terminal_text.insert(tk.END, line["text"] + "\n")

    def on_filter_change(self):
        self.terminal_filter = self.filter_entry.get().strip()
        self.refresh_terminal()

    def toggle_filter(self):
        self.filter_entry.focus_set()

    def toggle_changes(self):
        self.terminal_changes_only = not self.terminal_changes_only
        self.refresh_terminal()

    def toggle_pause(self):
        self.terminal_paused = not self.terminal_paused
        self.refresh_terminal()

    def refresh_history(self):
        self.history_text.delete(1.0, tk.END)
        for entry in reversed(self.history_lines):
            self.history_text.insert(
                tk.END,
                f"[{entry['timestamp']}] [{state_label(entry['state'])}] {entry['topic']}")
            if entry.get("payload"):
                self.history_text.insert(tk.END, f"  payload={entry['payload']}")
            self.history_text.insert(tk.END, "\n")

    def export_csv(self):
        import csv
        path = tk.filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if path:
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["timestamp", "topic", "state", "payload"])
                for entry in self.history_lines:
                    writer.writerow([entry["timestamp"], entry["topic"], state_label(entry["state"]), entry.get("payload", "")])

    def refresh_connection_bar(self):
        self.msg_rate_label.config(text=f"msgs/s: {self.last_msg_rate:.1f}")
        if self.connected:
            self.broker_state_label.config(text="Connected", fg="green")
        else:
            self.broker_state_label.config(text="Disconnected", fg="red")

    def refresh_table_stats(self):
        pass

    def setup_history(self):
        frame = tk.Frame(self.tab_history)
        frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        tk.Label(frame, text="State Change History").pack(anchor=tk.W)
        self.history_text = tk.Text(frame, height=20, wrap=tk.WORD)
        self.history_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.history_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.history_text.configure(yscrollcommand=scrollbar.set)
        btn = tk.Button(frame, text="Export CSV", command=self.export_csv)
        btn.pack(side=tk.BOTTOM, pady=5)

    def setup_settings(self):
        frame = tk.Frame(self.tab_settings)
        frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        tk.Label(frame, text="Broker Profiles").pack(anchor=tk.W)
        self.settings_text = tk.Text(frame, height=20, wrap=tk.WORD)
        self.settings_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.settings_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.settings_text.configure(yscrollcommand=scrollbar.set)
        self.settings_text.insert("1.0", json.dumps(self.profiles, indent=2))
        btn = tk.Button(frame, text="Save Profiles", command=self.save_profiles_ui)
        btn.pack(side=tk.BOTTOM, pady=5)

    # ----- Discovery mode ---------------------------------------------------
    def setup_discovery(self):
        self.tab_discovery = tk.Frame(self.notebook)
        self.notebook.add(self.tab_discovery, text="Discovery")
        ctrl = tk.Frame(self.tab_discovery)
        ctrl.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        self.discovery_btn = tk.Button(
            ctrl, text="Start Discovery", command=self.toggle_discovery)
        self.discovery_btn.pack(side=tk.LEFT, padx=5)
        tk.Button(ctrl, text="Add Selected",
                  command=self.discovery_add_selected).pack(side=tk.LEFT, padx=5)
        tk.Button(ctrl, text="Add All New",
                  command=self.discovery_add_all).pack(side=tk.LEFT, padx=5)
        self.discovery_status = tk.Label(
            ctrl, text="Idle - connect, then start discovery", fg="gray")
        self.discovery_status.pack(side=tk.LEFT, padx=10)

        frame = tk.Frame(self.tab_discovery)
        frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=5, pady=5)
        cols = ("topic", "class", "tier", "count", "avg_interval",
                "last_seen", "sample")
        self.discovery_tree = ttk.Treeview(frame, columns=cols,
                                           show="headings")
        for col, w, text in (
                ("topic", 320, "Topic"), ("class", 90, "Class"),
                ("tier", 60, "Tier"), ("count", 70, "Msgs"),
                ("avg_interval", 100, "Avg interval"),
                ("last_seen", 110, "Last seen"),
                ("sample", 300, "Sample payload")):
            self.discovery_tree.heading(col, text=text)
            self.discovery_tree.column(col, width=w, anchor=tk.W)
        self.discovery_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sbar = ttk.Scrollbar(frame, orient=tk.VERTICAL,
                             command=self.discovery_tree.yview)
        sbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.discovery_tree.configure(yscrollcommand=sbar.set)

    def toggle_discovery(self):
        if not self.discovery_active:
            if not self.connected or not getattr(self, "mqtt_client", None):
                tk.messagebox.showerror(
                    "Not connected",
                    "Connect to a broker before starting discovery.")
                return
            try:
                self.mqtt_client.subscribe("#")
            except Exception as e:
                tk.messagebox.showerror(
                    "Discovery", f"Could not subscribe to #: {e}")
                return
            self.discovery_active = True
            self.discovery_btn.config(text="Stop Discovery")
            self.discovery_status.config(
                text="Discovering - listening to '#' ...", fg="blue")
            log_message("INFO", "discovery started (subscribed to '#')")
        else:
            self.discovery_active = False
            try:
                self.mqtt_client.unsubscribe("#")
            except Exception:
                pass
            self.discovery_btn.config(text="Start Discovery")
            self.discovery_status.config(text="Stopped", fg="gray")
            log_message("INFO", "discovery stopped")

    def _record_discovery(self, topic, payload):
        now = time.time()
        rec = self.discovered.get(topic)
        if rec is None:
            rec = self.discovered[topic] = {
                "topic": topic, "count": 0, "stamps": [],
                "first": now, "last": now, "sample": payload[:120]}
        rec["count"] += 1
        rec["last"] = now
        rec["sample"] = payload[:120]
        stamps = rec["stamps"]
        if not stamps or now - stamps[-1] > 0.5:
            stamps.append(now)
            del stamps[:-120]

    def refresh_discovery_tree(self):
        for item in self.discovery_tree.get_children():
            self.discovery_tree.delete(item)
        now = time.time()
        for topic in sorted(self.discovered):
            r = self.discovered[topic]
            stamps = r["stamps"]
            if len(stamps) >= 2:
                gaps = [b - a for a, b in zip(stamps, stamps[1:])]
                avg = f"{sum(gaps) / len(gaps):.1f}s"
            else:
                avg = "-"
            cls = classify_topic(topic)
            self.discovery_tree.insert(
                "", tk.END,
                values=(topic, cls, guess_tier(cls), r["count"], avg,
                        human_delta(now - r["last"]), r["sample"]))

    def discovery_add_selected(self):
        sel = self.discovery_tree.selection()
        if not sel:
            tk.messagebox.showinfo(
                "Discovery", "Select one or more rows first.")
            return
        topics = [self.discovery_tree.item(i)["values"][0] for i in sel]
        self._discovery_save(topics)

    def discovery_add_all(self):
        self._discovery_save(list(self.discovered))

    def _discovery_save(self, topics):
        added = 0
        for t in topics:
            t = str(t)
            if t in self.state_machines:
                continue
            rec = self.discovered.get(t, {})
            stamps = rec.get("stamps", [])
            if len(stamps) >= 2:
                gaps = [b - a for a, b in zip(stamps, stamps[1:])]
                avg = max(1.0, round(sum(gaps) / len(gaps), 1))
            else:
                avg = 60.0
            cls = classify_topic(t)
            entry = {
                "topic": t,
                "class": cls,
                "tier": guess_tier(cls),
                "short_name": t.rsplit("/", 1)[-1],
                "purpose": "Discovered automatically via '#' subscription",
                "if_silent": "Review and set a sensible alert window",
                "expected_interval_sec": float(avg),
                "measured_interval_sec": float(avg),
                "last_message_at": None,
                "last_message_payload": None,
            }
            self.topics.append(entry)
            self.topic_details[t] = entry
            self.state_machines[t] = MQTTStateMachine(t, float(avg))
            added += 1
        if added:
            save_topics(self.topics)
            self.refresh_topic_table()
            log_message("INFO",
                        f"discovery saved {added} new topic(s) to topics.json")
        tk.messagebox.showinfo(
            "Discovery",
            f"Added {added} new topic(s). Total topics: {len(self.topics)}")

    def refresh_topic_table(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        sorted_topics = sorted(self.state_machines.items(), key=self.topic_sort_key)
        for topic, sm in sorted_topics:
            details = self.topic_details[topic]
            self.tree.insert("", tk.END, values=(
                topic,
                details.get("short_name", ""),
                details.get("class", ""),
                details.get("tier", 2),
                state_label(sm.state),
                human_delta(time.time() - sm.last_message_at) if sm.last_message_at else "never",
                f"{sm.measured_interval_sec():.1f}s",
                f"{details.get("expected_interval_sec", 60):.0f}s",
                self.activity_text(topic)
            ))

    def topic_sort_key(self, item):
        topic, sm = item
        details = self.topic_details.get(topic, {})
        state_order = {"ALIVE": 0, "STALE": 1, "DEAD": 2}
        return (details.get("tier", 2),
                state_order.get(state_label(sm.state), 99),
                topic)

    def activity_text(self, topic):
        with self.terminal_lock:
            recent = [l for l in self.terminal_lines if l["topic"] == topic and l["time"] > time.time() - 60]
        return f"{len(recent)} msgs"


    def add_terminal_line(self, topic, payload, state, changed=False):
        with self.terminal_lock:
            text = f"[{now_iso()}] {topic} -> {state_label(state)}"
            if payload:
                text += f" payload={payload}"
            entry = {"time": time.time(), "topic": topic, "state": state,
                     "changed": changed, "text": text}
            self.terminal_lines.append(entry)
            if len(self.terminal_lines) > 1000:
                self.terminal_lines = self.terminal_lines[-1000:]

    def save_profiles_ui(self):
        raw = self.settings_text.get("1.0", tk.END)
        try:
            data = json.loads(raw)
            if not isinstance(data, list) or not all(
                    isinstance(p, dict) and "name" in p for p in data):
                raise ValueError(
                    "Profiles must be a JSON list of objects, each with a 'name' field")
        except ValueError as e:
            tk.messagebox.showerror("Invalid profiles", str(e))
            return
        self.profiles = data
        save_profiles(self.profiles)
        names = [p.get("name") for p in self.profiles]
        if (self.current_profile is None
                or self.current_profile.get("name") not in names):
            self.current_profile = self.profiles[0] if self.profiles else None
        # Rebuild the profile dropdown to match the saved list
        menu = self.profile_menu["menu"]
        menu.delete(0, tk.END)
        for name in names:
            menu.add_command(label=name,
                             command=lambda n=name: self.on_profile_changed(n))
        tk.messagebox.showinfo(
            "Saved", f"{len(self.profiles)} profile(s) saved to profiles.json")

    def log_state_change(self, topic, payload, state):
        prev = self._last_logged_state.get(topic)
        if prev == state:
            return
        self._last_logged_state[topic] = state
        if prev is None and state == MQTTStateMachine.DEAD:
            return  # initial DEAD at startup - not a transition
        level = ("INFO" if state == MQTTStateMachine.ALIVE else
                 "WARN" if state == MQTTStateMachine.STALE else "ERROR")
        detail = f"topic={topic} state={state_label(state)}"
        if payload:
            detail += f" payload={payload}"
        log_message(level, detail)
        self.history_lines.append({"timestamp": now_iso(), "topic": topic,
                                   "state": state, "payload": payload})
        self.refresh_history()


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
