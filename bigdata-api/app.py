from flask import Flask, jsonify
from cassandra.cluster import Cluster
import happybase
import os
from datetime import datetime

app = Flask(__name__)

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "100.97.208.110")
HBASE_HOST = os.environ.get("HBASE_HOST", "100.97.208.110")
KEYSPACE = "cybersecurity"

def get_cassandra():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    return session, cluster

def get_hbase():
    return happybase.Connection(HBASE_HOST, port=9090)

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "RAPID API",
        "cassandra": CASSANDRA_HOST,
        "hbase": HBASE_HOST,
        "hbase_port": 9090,
        "keyspace": KEYSPACE
    })

@app.route("/threats/ip/<ip>")
def get_threat_by_ip(ip):
    result = {
        "ip": ip,
        "realtime": None,
        "score": None,
        "signatures": [],
        "volume_alerts": [],
        "historical_score": None,
        "threat_level": "UNKNOWN",
        "recommendation": "MONITOR"
    }

    try:
        session, cluster = get_cassandra()

        rows = list(session.execute(
            "SELECT * FROM realtime_threats WHERE ip_source=%s",
            [ip]
        ))
        if rows:
            r = rows[0]
            result["realtime"] = {
                "ip_source": r.ip_source,
                "last_seen": str(r.last_seen),
                "attack_types": r.attack_types,
                "threat_score": r.threat_score
            }

        rows = list(session.execute(
            "SELECT * FROM threat_scores WHERE source_ip=%s",
            [ip]
        ))
        if rows:
            r = rows[0]
            result["score"] = {
                "source_ip": r.source_ip,
                "score": r.score,
                "total_events": r.total_events,
                "malicious_count": r.malicious_count,
                "suspicious_count": r.suspicious_count,
                "last_seen": r.last_seen
            }

        rows = list(session.execute(
            "SELECT * FROM signature_alerts WHERE source_ip=%s",
            [ip]
        ))
        result["signatures"] = [{
            "timestamp": str(r.timestamp),
            "reason": r.reason,
            "request_path": r.request_path,
            "user_agent": r.user_agent,
            "threat_label": r.threat_label
        } for r in rows]

        rows = list(session.execute(
            "SELECT * FROM volume_alerts WHERE source_ip=%s",
            [ip]
        ))
        result["volume_alerts"] = [{
            "window_start": r.window_start,
            "window_end": r.window_end,
            "total_bytes": r.total_bytes,
            "threshold": r.threshold,
            "reason": r.reason
        } for r in rows]

        cluster.shutdown()

    except Exception as e:
        result["cassandra_error"] = str(e)

    try:
        conn = get_hbase()
        table = conn.table("ip_reputation")
        row = table.row(ip.encode())
        if row:
            score = row.get(b"cf:reputation_score", b"0")
            result["historical_score"] = float(score.decode())
        conn.close()
    except Exception as e:
        result["hbase_error"] = str(e)

    raw_score = 0
    if result["score"]:
        raw_score = result["score"]["score"] or 0
    elif result["realtime"]:
        raw_score = result["realtime"]["threat_score"] or 0

    if raw_score >= 70:
        result["threat_level"] = "HIGH"
        result["recommendation"] = "BLOCK"
    elif raw_score >= 40:
        result["threat_level"] = "MEDIUM"
        result["recommendation"] = "MONITOR"
    else:
        result["threat_level"] = "LOW"
        result["recommendation"] = "ALLOW"

    return jsonify(result)

@app.route("/threats/top10")
def get_top10():
    try:
        session, cluster = get_cassandra()
        rows = session.execute(
            "SELECT source_ip, score, malicious_count, suspicious_count, total_events, last_seen FROM threat_scores"
        )

        data = []
        for r in rows:
            data.append({
                "ip": r.source_ip,
                "score": r.score,
                "malicious_count": r.malicious_count,
                "suspicious_count": r.suspicious_count,
                "total_events": r.total_events,
                "last_seen": r.last_seen
            })

        cluster.shutdown()
        data = sorted(data, key=lambda x: x["score"] or 0, reverse=True)[:10]
        return jsonify({"top10": data, "count": len(data)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/threshold")
def get_threshold():
    try:
        session, cluster = get_cassandra()
        rows = session.execute("SELECT score FROM threat_scores")

        scores = [r.score for r in rows if r.score is not None]
        cluster.shutdown()

        if not scores:
            return jsonify({
                "threshold": 50,
                "avg_score": 0,
                "total_ips": 0
            })

        avg = sum(scores) / len(scores)
        threshold = min(avg * 1.5, 100)

        return jsonify({
            "threshold": round(threshold, 2),
            "avg_score": round(avg, 2),
            "total_ips": len(scores),
            "computed_at": datetime.utcnow().isoformat()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/recent")
def get_recent():
    try:
        session, cluster = get_cassandra()
        rows = session.execute(
            "SELECT source_ip, timestamp, reason, request_path, user_agent, threat_label FROM signature_alerts LIMIT 10"
        )

        data = [{
            "ip": r.source_ip,
            "timestamp": str(r.timestamp),
            "reason": r.reason,
            "request_path": r.request_path,
            "user_agent": r.user_agent,
            "threat_label": r.threat_label
        } for r in rows]

        cluster.shutdown()
        return jsonify({"recent": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/volume-alerts")
def get_volume_alerts():
    try:
        session, cluster = get_cassandra()
        rows = session.execute(
            "SELECT source_ip, window_start, window_end, total_bytes, threshold, reason FROM volume_alerts LIMIT 20"
        )

        data = [{
            "ip": r.source_ip,
            "window_start": r.window_start,
            "window_end": r.window_end,
            "total_bytes": r.total_bytes,
            "threshold": r.threshold,
            "reason": r.reason
        } for r in rows]

        cluster.shutdown()
        return jsonify({"volume_alerts": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/by-protocol")
def get_by_protocol():
    try:
        session, cluster = get_cassandra()
        rows = session.execute(
            "SELECT protocol, threat_label FROM logs LIMIT 5000"
        )

        by_protocol = {}
        for r in rows:
            protocol = r.protocol or "UNKNOWN"
            if protocol not in by_protocol:
                by_protocol[protocol] = {
                    "total": 0,
                    "malicious": 0,
                    "suspicious": 0
                }

            by_protocol[protocol]["total"] += 1

            if r.threat_label == "malicious":
                by_protocol[protocol]["malicious"] += 1
            elif r.threat_label == "suspicious":
                by_protocol[protocol]["suspicious"] += 1

        cluster.shutdown()
        return jsonify({"by_protocol": by_protocol})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/timeline")
def get_timeline():
    try:
        session, cluster = get_cassandra()
        rows = session.execute(
            "SELECT timestamp, threat_label FROM logs LIMIT 5000"
        )

        by_day = {}
        for r in rows:
            if not r.timestamp:
                continue

            day = str(r.timestamp)[:10]

            if day not in by_day:
                by_day[day] = {
                    "date": day,
                    "total": 0,
                    "malicious": 0,
                    "suspicious": 0
                }

            by_day[day]["total"] += 1

            if r.threat_label == "malicious":
                by_day[day]["malicious"] += 1
            elif r.threat_label == "suspicious":
                by_day[day]["suspicious"] += 1

        cluster.shutdown()
        timeline = sorted(by_day.values(), key=lambda x: x["date"])
        return jsonify({"timeline": timeline, "days": len(timeline)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
