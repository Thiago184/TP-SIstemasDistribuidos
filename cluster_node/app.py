from flask import Flask, request, jsonify
import requests
import os
import time
import random
import threading

app = Flask(__name__)

PEER_ID = os.getenv("PEER_ID")
PEERS = os.getenv("PEERS").split(",")

lock = threading.Lock()

current_request = None
ok_received = set()
deferred_requests = []
in_critical_section = False


def timestamp():
    return time.time()


def has_priority(req1, req2):
    if req1["timestamp"] < req2["timestamp"]:
        return True
    if req1["timestamp"] == req2["timestamp"] and req1["peer_id"] < req2["peer_id"]:
        return True
    return False


@app.route("/request_resource", methods=["POST"])
def request_resource():
    global current_request, ok_received, in_critical_section

    data = request.json
    client_id = data["client_id"]
    request_id = data["request_id"]
    client_timestamp = data.get("client_timestamp")

    ts = timestamp()

    my_request = {
        "client_id": client_id,
        "request_id": request_id,
        "client_timestamp": client_timestamp,
        "timestamp": ts,
        "peer_id": PEER_ID
    }

    with lock:
        current_request = my_request
        ok_received = {PEER_ID}

    print(
        f"[{PEER_ID}] Pedido recebido de {client_id} | "
        f"client_timestamp={client_timestamp} | peer_timestamp={ts}"
    )

    for peer in PEERS:
        if peer != f"http://{PEER_ID}:5000":
            try:
                requests.post(f"{peer}/peer_request", json=my_request, timeout=5)
            except Exception as e:
                print(f"[{PEER_ID}] Erro ao enviar pedido para {peer}: {e}")

    while True:
        with lock:
            if len(ok_received) == len(PEERS):
                in_critical_section = True
                break
        time.sleep(0.1)

    enter_critical_section(my_request)

    with lock:
        in_critical_section = False
        current_request = None

    release_deferred_requests(my_request)

    return jsonify({
        "status": "COMMITTED",
        "peer": PEER_ID,
        "client_id": client_id,
        "request_id": request_id,
        "client_timestamp": client_timestamp,
        "peer_timestamp": ts
    })


@app.route("/peer_request", methods=["POST"])
def peer_request():
    data = request.json
    requester_peer = data["peer_id"]

    send_ok = False

    with lock:
        if in_critical_section:
            deferred_requests.append(data)

        elif current_request is None:
            send_ok = True

        elif has_priority(data, current_request):
            send_ok = True

        else:
            deferred_requests.append(data)

    if send_ok:
        try:
            requests.post(
                f"http://{requester_peer}:5000/ok",
                json={"from": PEER_ID},
                timeout=5
            )
            print(f"[{PEER_ID}] OK enviado para {requester_peer}")
        except Exception as e:
            print(f"[{PEER_ID}] Erro ao enviar OK para {requester_peer}: {e}")
    else:
        print(f"[{PEER_ID}] Pedido de {requester_peer} adiado")

    return jsonify({"status": "RECEIVED"})


@app.route("/ok", methods=["POST"])
def receive_ok():
    data = request.json
    sender = data["from"]

    with lock:
        ok_received.add(sender)

    print(f"[{PEER_ID}] OK recebido de {sender}")

    return jsonify({"status": "OK_RECEIVED"})


def enter_critical_section(req):
    print(f"[{PEER_ID}] Entrando na seção crítica para {req['client_id']}")

    sleep_time = random.uniform(0.2, 1.0)

    with open("recurso_R.txt", "a") as file:
        file.write(
            f"ENTER_CS | peer={PEER_ID} | client={req['client_id']} | "
            f"request={req['request_id']} | "
            f"client_timestamp={req.get('client_timestamp')} | "
            f"timestamp={time.time()}\n"
        )
        file.flush()

        time.sleep(sleep_time)

        file.write(
            f"EXIT_CS | peer={PEER_ID} | client={req['client_id']} | "
            f"request={req['request_id']} | "
            f"client_timestamp={req.get('client_timestamp')} | "
            f"timestamp={time.time()}\n"
        )

        file.write(
            f"COMMITTED | peer={PEER_ID} | client={req['client_id']} | "
            f"request={req['request_id']} | "
            f"client_timestamp={req.get('client_timestamp')} | "
            f"timestamp={time.time()}\n"
        )
        file.flush()

    print(f"[{PEER_ID}] Saindo da seção crítica")


def send_ok_to_deferred_request(req, message):
    requester_peer = req["peer_id"]

    try:
        requests.post(
            f"http://{requester_peer}:5000/ok",
            json={"from": PEER_ID},
            timeout=5
        )
        print(f"[{PEER_ID}] {message} {requester_peer} | timestamp={req['timestamp']}")
    except Exception as e:
        print(f"[{PEER_ID}] Erro ao liberar OK para {requester_peer}: {e}")


def release_deferred_requests(completed_request):
    global deferred_requests

    with lock:
        requests_to_release = sorted(
            deferred_requests,
            key=lambda req: (req["timestamp"], req["peer_id"])
        )
        deferred_requests = []

    successor_requests = [
        req for req in requests_to_release
        if not has_priority(req, completed_request)
    ]

    if successor_requests:
        immediate_successor = successor_requests[0]
        send_ok_to_deferred_request(
            immediate_successor,
            "OK liberado para sucessor temporal imediato"
        )

        for req in successor_requests[1:]:
            send_ok_to_deferred_request(
                req,
                "OK liberado para pedido adiado seguinte"
            )


if __name__ == "__main__":
    print(f"Iniciando {PEER_ID}")
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    app.run(host="0.0.0.0", port=5000, threaded=True)
