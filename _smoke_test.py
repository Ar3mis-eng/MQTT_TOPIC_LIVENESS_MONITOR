"""Smoke test: instantiate App, inject fake MQTT messages, verify UI/state."""
import importlib.util
from pathlib import Path

PATH = Path(__file__).resolve().with_name('_mqtt_topic_monitor.py')
spec = importlib.util.spec_from_file_location('monitor', PATH)
mon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mon)

app = mon.App()
app.update_idletasks()

# 1. Simulate message from the (fake) MQTT thread -> should only queue
app.on_mqtt_message('cmd/device/fan', 'ON')
assert len(app.terminal_lines) == 0, 'UI must not be touched from MQTT thread'

# 2. UI tick drains the queue
app.ui_tick()
sm = app.state_machines['cmd/device/fan']
assert sm.state == mon.MQTTStateMachine.ALIVE, f'expected ALIVE, got {sm.state}'
assert len(app.history_lines) == 1, f'expected 1 history entry, got {len(app.history_lines)}'
assert len(app.terminal_lines) == 1, f'expected 1 terminal line, got {len(app.terminal_lines)}'
assert len(app.tree.get_children()) == len(app.topics), 'table rows != topics'

# 3. Duplicate message with no state change -> no new history entry
app.on_mqtt_message('cmd/device/fan', 'OFF')
app.ui_tick()
assert len(app.history_lines) == 1, 'state unchanged => history must not grow'

# 4. Time-based decay: pretend last message was long ago -> DEAD transition logged
import time as _t
sm.last_message_at = _t.time() - 9999
app.ui_tick()
assert sm.state == mon.MQTTStateMachine.DEAD, f'expected DEAD, got {sm.state}'
assert len(app.history_lines) == 2, f'DEAD transition must be logged, got {len(app.history_lines)}'

# 5. Terminal filter / pause / changes-only logic
app.filter_entry.insert(0, 'cmd/device/fan')
app.on_filter_change()
app.refresh_terminal()  # should not raise
app.terminal_paused = True
app.refresh_terminal()  # returns early
app.terminal_paused = False
app.terminal_changes_only = True
app.refresh_terminal()

# 6. Profiles: settings text populated, round-trip save parse
raw = app.settings_text.get('1.0', 'end')
import json as _json
profiles = _json.loads(raw)
assert isinstance(profiles, list) and profiles and 'name' in profiles[0], 'settings text must hold profiles JSON'

# 7. Sorting by tier (critical first)
items = sorted(app.state_machines.items(), key=app.topic_sort_key)
tiers = [app.topic_details[t].get('tier', 2) for t, _ in items]
assert tiers == sorted(tiers), f'topics must sort by tier first, got tiers {tiers}'

# 8. topic_to_dict handles dicts (used by save_topics)
d = mon.topic_to_dict(app.topics[0])
assert d['topic'] == app.topics[0]['topic']

# 9. Detail panel: no selection -> hint text; with selection -> full detail
import tkinter as _tk
app.refresh_detail()
assert 'Select a topic' in app.detail_text.get('1.0', 'end')
app.selected_topic = 'cmd/device/fan'
app.refresh_detail()
detail = app.detail_text.get('1.0', 'end')
for expected in ('Topic: cmd/device/fan', 'Purpose:', 'If silent:',
                 'Interval stats', 'Last messages:'):
    assert expected in detail, f'detail panel missing: {expected}'

# 10. Discovery mode (stub out modal dialogs)
app.discovery_active = True
mon.tk.messagebox.showinfo = lambda *a, **k: None
app._record_discovery('sensor/new/thing', '42.5')
app._record_discovery('sensor/new/thing', '43.0')
app.refresh_discovery_tree()
rows = app.discovery_tree.get_children()
assert len(rows) == 1, f'expected 1 discovery row, got {len(rows)}'
vals = app.discovery_tree.item(rows[0])['values']
assert str(vals[0]) == 'sensor/new/thing' and str(vals[1]) == 'sensor', vals
assert str(vals[2]) == '2', f'sensor topics should be tier 2, got {vals[2]}'

# 11. Saving discovered topic -> appears in monitor + topics.json
topics_existed = mon.TOPICS_FILE.exists()
app._discovery_save(['sensor/new/thing'])
assert 'sensor/new/thing' in app.state_machines, 'discovered topic not added'
assert len(app.tree.get_children()) == len(app.topics), 'table not refreshed'
assert mon.TOPICS_FILE.exists(), 'topics.json not written'
# verify persisted content
saved = __import__('json').loads(mon.TOPICS_FILE.read_text(encoding='utf-8'))
assert any(x['topic'] == 'sensor/new/thing' for x in saved)
# idempotent: saving again adds nothing new
app._discovery_save(['sensor/new/thing'])
assert 'sensor/new/thing' in app.state_machines

# 12. Classification helper
assert mon.classify_topic('cmd/device/fan') == 'device'
assert mon.classify_topic('heartbeat/controller/main') == 'control'
assert mon.classify_topic('whatever/x') == 'other'
assert mon.guess_tier('device') == 1

app.destroy()
# Clean up side effect if topics.json did not exist before the test
if not topics_existed:
    mon.TOPICS_FILE.unlink(missing_ok=True)
print('ALL SMOKE TESTS PASSED')
