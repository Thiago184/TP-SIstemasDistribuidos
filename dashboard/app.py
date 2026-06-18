from collections import Counter
from pathlib import Path
import re
import statistics

from flask import Flask, jsonify, render_template


app = Flask(__name__)

RESOURCE_PATH = Path("recurso_R.txt")
LOG_PATH = Path("logs_execucao.txt")

RESOURCE_EVENT_RE = re.compile(
    r"^(ENTER_CS|EXIT_CS|COMMITTED) \| "
    r"peer=([^|]+) \| client=([^|]+) \| request=([^|]+) \|"
    r"(?: client_timestamp=([^|]+) \|)? timestamp=([0-9.]+)"
)

ERROR_RE = re.compile(r"\b(Erro|ERROR|Traceback|timeout|Read timed out|Exception)\b", re.I)


def read_lines(path):
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def parse_resource():
    lines = read_lines(RESOURCE_PATH)
    counts = Counter()
    per_client = Counter()
    per_peer = Counter()
    latest_events = []
    malformed = []
    missing_client_timestamp = []
    overlaps = []
    bad_sequence = []
    durations = []
    active = None
    active_request = None
    entered_requests = set()
    exited_requests = set()
    committed_requests = set()

    for line_number, line in enumerate(lines, 1):
        match = RESOURCE_EVENT_RE.match(line)
        if not match:
            if line.strip():
                malformed.append({"line": line_number, "text": line})
            continue

        event_type, peer, client, request_id, client_timestamp, timestamp = match.groups()
        timestamp = float(timestamp)
        counts[event_type] += 1

        event = {
            "line": line_number,
            "type": event_type,
            "peer": peer.strip(),
            "client": client.strip(),
            "request": request_id.strip(),
            "client_timestamp": None if client_timestamp in (None, "None", "") else client_timestamp.strip(),
            "timestamp": timestamp,
        }
        latest_events.append(event)

        if event["client_timestamp"] is None:
            missing_client_timestamp.append(event)

        if event_type == "ENTER_CS":
            entered_requests.add(event["request"])
            per_client[event["client"]] += 1
            per_peer[event["peer"]] += 1
            if active is not None:
                overlaps.append({
                    "line": line_number,
                    "current": event,
                    "active": active,
                })
            active = event
            active_request = event["request"]

        elif event_type == "EXIT_CS":
            exited_requests.add(event["request"])
            if active is None:
                bad_sequence.append({
                    "line": line_number,
                    "problem": "EXIT_CS sem ENTER_CS ativo",
                    "event": event,
                })
            elif event["request"] != active_request:
                bad_sequence.append({
                    "line": line_number,
                    "problem": f"EXIT_CS de {event['request']} difere do ativo {active_request}",
                    "event": event,
                })
                active = None
                active_request = None
            else:
                durations.append(event["timestamp"] - active["timestamp"])
                active = None
                active_request = None

        elif event_type == "COMMITTED":
            committed_requests.add(event["request"])

    incomplete_exit = sorted(entered_requests - exited_requests)
    incomplete_commit = sorted(entered_requests - committed_requests)
    counts_equal = counts["ENTER_CS"] == counts["EXIT_CS"] == counts["COMMITTED"]
    has_events = sum(counts.values()) > 0
    resource_ok = (
        has_events
        and counts_equal
        and not overlaps
        and not bad_sequence
        and active is None
        and not malformed
        and not missing_client_timestamp
        and not incomplete_exit
        and not incomplete_commit
    )

    return {
        "line_count": len(lines),
        "counts": {
            "ENTER_CS": counts["ENTER_CS"],
            "EXIT_CS": counts["EXIT_CS"],
            "COMMITTED": counts["COMMITTED"],
        },
        "per_client": dict(sorted(per_client.items())),
        "per_peer": dict(sorted(per_peer.items())),
        "latest_events": latest_events[-30:][::-1],
        "overlap_count": len(overlaps),
        "bad_sequence_count": len(bad_sequence),
        "malformed_count": len(malformed),
        "missing_client_timestamp_count": len(missing_client_timestamp),
        "active_at_end": active is not None,
        "incomplete_exit_count": len(incomplete_exit),
        "incomplete_commit_count": len(incomplete_commit),
        "duration": {
            "min": min(durations) if durations else None,
            "max": max(durations) if durations else None,
            "avg": statistics.mean(durations) if durations else None,
        },
        "samples": {
            "overlaps": overlaps[:3],
            "bad_sequence": bad_sequence[:3],
            "malformed": malformed[:3],
            "missing_client_timestamp": missing_client_timestamp[:3],
            "incomplete_exit": incomplete_exit[:5],
            "incomplete_commit": incomplete_commit[:5],
        },
        "ok": resource_ok,
        "has_events": has_events,
    }


def parse_logs():
    lines = read_lines(LOG_PATH)
    text = "\n".join(lines)
    waits = [int(value) for value in re.findall(r"Aguardando ([1-5])s antes", text)]
    commits_by_client = Counter(re.findall(r"\[(client\d+)\] COMMITTED recebido", text))
    finalized_clients = sorted(set(re.findall(r"\[(client\d+)\] Finalizou todos os pedidos", text)))
    error_matches = ERROR_RE.findall(text)

    return {
        "line_count": len(lines),
        "client_timestamp_count": text.count("client_timestamp"),
        "committed_received_count": sum(commits_by_client.values()),
        "commits_by_client": dict(sorted(commits_by_client.items())),
        "finalized_clients": finalized_clients,
        "finalized_count": len(finalized_clients),
        "successor_immediate_count": text.count("sucessor temporal imediato"),
        "deferred_following_count": text.count("pedido adiado seguinte"),
        "wait_count": len(waits),
        "wait_min": min(waits) if waits else None,
        "wait_max": max(waits) if waits else None,
        "error_count": len(error_matches),
        "recent_errors": [
            line for line in lines[-300:] if ERROR_RE.search(line)
        ][-10:],
    }


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def summary():
    resource = parse_resource()
    logs = parse_logs()
    overall_ok = resource["ok"] and logs["error_count"] == 0

    return jsonify({
        "overall_ok": overall_ok,
        "status": "OK" if overall_ok else "ATENCAO",
        "resource": resource,
        "logs": logs,
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
