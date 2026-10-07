"""Quick LAN scan for an MQTT broker (TCP 1883) - diagnostic helper."""
import socket
import concurrent.futures

SUBNET = "192.168.1"
PORT = 1883
TIMEOUT = 0.8


def check(ip):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    try:
        s.connect((ip, PORT))
        s.close()
        return ip
    except Exception:
        return None


ips = [f"{SUBNET}.{i}" for i in range(1, 255)]
found = []
with concurrent.futures.ThreadPoolExecutor(max_workers=64) as ex:
    for result in ex.map(check, ips):
        if result:
            found.append(result)

if found:
    print("MQTT broker candidates (port 1883 open):")
    for ip in found:
        print(f"  {ip}:{PORT}")
else:
    print(f"No host with open port {PORT} found in {SUBNET}.0/24")
