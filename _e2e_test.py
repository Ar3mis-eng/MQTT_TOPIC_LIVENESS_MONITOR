"""Live E2E test: connect the App to test.mosquitto.org, publish from a
separate helper client, and verify state/history/discovery all work."""
import importlib.util
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

PATH = Path(__file__).resolve().with_name('_mqtt_topic_monitor.py')
spec = importlib.util.spec_from_file_location('monitor', PATH)
mon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mon)

app = mon.App()

# Point the app at the public test broker
app.current_profile = {
    'name': 'Public test broker', 'address': 'test.mosquitto.org',
    'port': 1883, 'user': '', 'password': '',
}
app.connect()
assert app.connected, 'could not connect to test.mosquitto.org'
print('CONNECTED to test.mosquitto.org; subscribed to',
      len(app.topics), 'known topics')

# Helper publisher: separate client, never the monitor itself
helper = mqtt.Client(CallbackAPIVersion.VERSION1, client_id='e2e-helper')
helper.connect('test.mosquitto.org', 1883, keepalive=30)
helper.loop_start()
stop = threading.Event()


def publish_loop():
    i = 0
    while not stop.is_set():
        helper.publish('cmd/device/fan', f'e2e-payload-{i}')
        helper.publish('e2e/discovery/probe', f'probe-{i}')
        i += 1
        time.sleep(0.5)


threading.Thread(target=publish_loop, daemon=True).start()


def pump(seconds):
    deadline = time.time() + seconds
    while time.time() < deadline:
        app.update()
        time.sleep(0.05)


pump(6)

sm = app.state_machines['cmd/device/fan']
print('state =', mon.state_label(sm.state), ' msg_count =', app.msg_count)
assert sm.state == mon.MQTTStateMachine.ALIVE, 'expected ALIVE after publishes'
assert app.msg_count > 0, 'no messages received'
assert app.history_lines, 'no history entry recorded'
assert any(l['topic'] == 'cmd/device/fan' for l in app.terminal_lines), \
    'no terminal lines for published topic'

# Detail panel with live data
app.selected_topic = 'cmd/device/fan'
pump(1)
detail = app.detail_text.get('1.0', 'end')
assert 'Topic: cmd/device/fan' in detail and 'Last messages:' in detail
assert 'e2e-payload' in detail, 'detail panel should show recent payloads'
print('DETAIL PANEL OK')

# Discovery against the live broker
app.discovery_active = True
app.mqtt_client.subscribe('#')
pump(5)
print('discovered', len(app.discovered), 'topic(s):',
      sorted(app.discovered)[:8])
assert 'e2e/discovery/probe' in app.discovered, \
    'discovery did not capture the probe topic'
app.refresh_discovery_tree()
assert app.discovery_tree.get_children(), 'discovery tree empty'

# Stop discovery (unsubscribes '#')
app.toggle_discovery()
assert not app.discovery_active

stop.set()
helper.loop_stop()
app.disconnect()
app.destroy()
print('E2E TEST PASSED')
