# MQTT Topic Liveness Monitor

A Tkinter desktop application that monitors MQTT topics and classifies them as alive, stale, or dead based on message timing. It listens to the broker and does not publish messages.

## Requirements

- Python 3.10 or newer
- Tkinter (usually included with Python)
- An MQTT broker reachable from your machine

## Setup

```powershell
python -m pip install -r requirements.txt
python _mqtt_topic_monitor.py
```

The default profile connects to an MQTT broker on `localhost:1883`. Configure broker profiles and monitored topics in the app. Saved profiles, topics, and logs stay local and are excluded from version control.

## Tests

Run the local smoke test with:

```powershell
python _smoke_test.py
```

`_e2e_test.py` is an optional integration test that uses the public Mosquitto test broker and publishes test messages there. Do not send private or production data to a public broker.