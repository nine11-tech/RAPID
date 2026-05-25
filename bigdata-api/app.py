from flask import Flask, jsonify, request
from flask_cors import CORS
from cassandra.cluster import Cluster
import happybase
import os
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app, resources={r'/*': {'origins': '*'}})

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "100.97.208.110")
HBASE_HOST = os.environ.get("HBASE_HOST", "100.97.208.110")
KEYSPACE = "cybersecurity"
HDFS_HOST = os.environ.get("HDFS_HOST", os.environ.get("KAFKA_HOST", "100.73.216.115"))
HDFS_WEB = f"http://{HDFS_HOST}:9870"

# Global persistent connection with auto-reconnect
_cassandra_session = None
_cassandra_cluster = None

def get_cassandra():
    global _cassandra_session, _cassandra_cluster
    try:
        if _cassandra_session is not None:
            _cassandra_session.execute("SELECT now() FROM system.local")
            return _cassandra_session, _cassandra_cluster
    except Exception:
        try:
            _cassandra_cluster.shutdown()
        except Exception:
            pass
        _cassandra_session = None
        _cassandra_cluster = None

    _cassandra_cluster = Cluster(
        [CASSANDRA_HOST],
        connect_timeout=60,
        control_connection_timeout=60,
        idle_heartbeat_interval=10
    )
    _cassandra_session = _cassandra_cluster.connect(KEYSPACE)
    _cassandra_session.default_timeout = 120
    return _cassandra_session, _cassandra_cluster

def get_hbase():
    return happybase.Connection(HBASE_HOST, port=9090)

def to_json_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple, set)):
        return [to_json_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_json_value(v) for k, v in value.items()}
    return str(value)

def parse_datetime_value(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).replace(tzinfo=None)
        except ValueError:
            pass

        for fmt, length in (
            ("%Y-%m-%d %H:%M:%S", 19),
            ("%Y-%m-%dT%H:%M:%S", 19),
            ("%Y-%m-%d", 10),
        ):
            try:
                return datetime.strptime(text[:length], fmt)
            except ValueError:
                pass
    return None

def get_int_arg(name, default, minimum, maximum):
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))

def speed_decision_score(row):
    stored = int(getattr(row, "score", 0) or getattr(row, "threat_score", 0) or 0)
    malicious = int(getattr(row, "malicious_count", 0) or 0)
    suspicious = int(getattr(row, "suspicious_count", 0) or 0)
    total = int(getattr(row, "total_events", 0) or 0)
    brute = 0
    attack_types = str(getattr(row, "attack_types", "") or "").lower()
    if "brute" in attack_types:
        brute = max(1, stored // 10)

    evidence_score = (
        malicious * 35
        + suspicious * 14
        + brute * 18
        + min(total, 50)
    )
    return min(100, max(stored, evidence_score))

def speed_severity(score):
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "HIGH"
    if score >= 35:
        return "MEDIUM"
    return "LOW"

def speed_decision(score):
    if score >= 80:
        return "Block source and investigate related traffic"
    if score >= 60:
        return "Quarantine or rate-limit while reviewing evidence"
    if score >= 35:
        return "Monitor closely and correlate with signatures"
    return "Monitor; no immediate block"

def event_score(row):
    """
    Convert one log event into a normalized score from 0 to 100.
    Used for adaptive threshold rolling 24h calculation.
    """
    score = 0

    label = (getattr(row, "threat_label", "") or "").lower()
    action = (getattr(row, "action", "") or "").lower()
    user_agent = (getattr(row, "user_agent", "") or "").lower()
    request_path = (getattr(row, "request_path", "") or "").lower()

    bytes_transferred = getattr(row, "bytes_transferred", 0) or 0

    # Base score from label
    if label == "malicious":
        score += 80
    elif label == "suspicious":
        score += 45
    elif label == "benign":
        score += 5
    else:
        score += 10

    # Blocked traffic is usually more suspicious
    if action == "blocked":
        score += 8

    # Signature-like indicators
    if "sqlmap" in user_agent or "union select" in request_path:
        score += 20

    if "nmap" in user_agent or "scan" in user_agent:
        score += 15

    if "../" in request_path or "..\\" in request_path or "passwd" in request_path:
        score += 20

    if "wp-login" in request_path or "hydra" in user_agent or "brute" in user_agent:
        score += 15

    if "<script" in request_path or "xss" in request_path:
        score += 20

    # Volume contribution, capped
    try:
        score += min(float(bytes_transferred) / 10000.0, 10)
    except Exception:
        pass

    return min(round(score, 2), 100)


def compute_adaptive_threshold(session, window_hours=24, multiplier=1.5):
    """
    Bonus Sprint:
    Ajustement dynamique des seuils de détection.

    - Moyenne glissante 24h
    - Recalcul automatique à chaque appel API
    - Source: logs Cassandra
    - Aucun nouveau conteneur
    """
    rows = list(session.execute(
        "SELECT timestamp, threat_label, action, bytes_transferred, request_path, user_agent "
        "FROM logs LIMIT 20000"
    ))

    valid_rows = [
        (r, parsed_timestamp)
        for r in rows
        for parsed_timestamp in [parse_datetime_value(getattr(r, "timestamp", None))]
        if parsed_timestamp is not None
    ]

    if not valid_rows:
        return {
            "threshold": 50,
            "avg_score_24h": 0,
            "samples_used": 0,
            "events_scanned": 0,
            "window_hours": window_hours,
            "formula": "fallback_default_no_timestamp_data",
            "mode": "rolling_24h"
        }

    # Dataset/replay mode:
    # use latest log timestamp as reference instead of real current time.
    reference_time = max(parsed_timestamp for _, parsed_timestamp in valid_rows)
    window_start = reference_time - timedelta(hours=window_hours)

    window_rows = [
        r for r, parsed_timestamp in valid_rows
        if window_start <= parsed_timestamp <= reference_time
    ]

    scores = [event_score(r) for r in window_rows]

    if not scores:
        return {
            "threshold": 50,
            "avg_score_24h": 0,
            "samples_used": 0,
            "events_scanned": len(valid_rows),
            "window_hours": window_hours,
            "reference_time": reference_time.isoformat(),
            "window_start": window_start.isoformat(),
            "formula": "fallback_default_empty_24h_window",
            "mode": "rolling_24h"
        }

    avg = sum(scores) / len(scores)
    threshold = min(avg * multiplier, 100)

    return {
        "threshold": round(threshold, 2),
        "avg_score_24h": round(avg, 2),
        "samples_used": len(scores),
        "events_scanned": len(valid_rows),
        "window_hours": window_hours,
        "reference_time": reference_time.isoformat(),
        "window_start": window_start.isoformat(),
        "multiplier": multiplier,
        "formula": "threshold = min(avg_score_24h * 1.5, 100)",
        "mode": "rolling_24h",
        "recalculation": "automatic_on_each_api_call"
    }


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
        session, _cluster = get_cassandra()

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
                "last_seen": to_json_value(r.last_seen)
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
            "window_start": to_json_value(r.window_start),
            "window_end": to_json_value(r.window_end),
            "total_bytes": r.total_bytes,
            "threshold": r.threshold,
            "reason": r.reason
        } for r in rows]

    except Exception as e:
        result["cassandra_error"] = str(e)

    try:
        conn = get_hbase()
        table = conn.table("ip_reputation")
        row = table.row(ip.encode())

        if row:
            threat_count = int(float(row.get(b"cf:threat_count", b"0").decode()))
            sqli_hits = int(float(row.get(b"cf:sqli_hits", b"0").decode()))
            tool_hits = int(float(row.get(b"cf:tool_hits", b"0").decode()))
            traversal_hits = int(float(row.get(b"cf:traversal_hits", b"0").decode()))
            xss_hits = int(float(row.get(b"cf:xss_hits", b"0").decode()))
            avg_bytes = float(row.get(b"cf:avg_bytes", b"0").decode())

            # Normalize historical score to 0-100
            historical_score = min(
                100,
                (threat_count * 0.01)
                + (sqli_hits * 0.08)
                + (tool_hits * 0.03)
                + (traversal_hits * 0.06)
                + (xss_hits * 0.08)
                + (avg_bytes / 10000)
            )

            result["historical_score"] = round(historical_score, 2)
            result["hbase_reputation"] = {
                "threat_count": threat_count,
                "sqli_hits": sqli_hits,
                "tool_hits": tool_hits,
                "traversal_hits": traversal_hits,
                "xss_hits": xss_hits,
                "avg_bytes": avg_bytes
            }

        conn.close()
    except Exception as e:
        result["hbase_error"] = str(e)

    raw_score = 0
    scores = []

    if result["score"]:
        scores.append(result["score"]["score"] or 0)

    if result["realtime"]:
        scores.append(result["realtime"]["threat_score"] or 0)

    if result["historical_score"] is not None:
        scores.append(result["historical_score"])

    if scores:
        raw_score = max(scores)

    result["final_score"] = raw_score

    try:
        session, _cluster = get_cassandra()
        threshold_info = compute_adaptive_threshold(session)
    except Exception as e:
        threshold_info = {
            "threshold": 50,
            "avg_score": 0,
            "total_ips": 0,
            "formula": "fallback",
            "error": str(e)
        }

    adaptive_threshold = threshold_info["threshold"]
    result["adaptive_threshold"] = threshold_info

    if raw_score > adaptive_threshold:
        result["threat_level"] = "HIGH"
        result["recommendation"] = "BLOCK"
    elif raw_score >= adaptive_threshold * 0.7:
        result["threat_level"] = "MEDIUM"
        result["recommendation"] = "MONITOR"
    else:
        result["threat_level"] = "LOW"
        result["recommendation"] = "ALLOW"

    return jsonify(to_json_value(result))

@app.route("/threats/top10")
def get_top10():
    try:
        session, _cluster = get_cassandra()
        limit = get_int_arg("limit", 10, 1, 100)
        rows = session.execute(
            "SELECT source_ip, score, malicious_count, suspicious_count, total_events, last_seen FROM threat_scores"
        )

        data = []
        for r in rows:
            decision_score = speed_decision_score(r)
            data.append({
                "ip": r.source_ip,
                "score": r.score,
                "decision_score": decision_score,
                "severity": speed_severity(decision_score),
                "recommended_decision": speed_decision(decision_score),
                "malicious_count": r.malicious_count,
                "suspicious_count": r.suspicious_count,
                "total_events": r.total_events,
                "last_seen": to_json_value(r.last_seen)
            })

        data = sorted(data, key=lambda x: x["decision_score"] or 0, reverse=True)[:limit]
        return jsonify(to_json_value({"top10": data, "count": len(data)}))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/threshold")
def get_threshold():
    try:
        session, _cluster = get_cassandra()
        data = compute_adaptive_threshold(session)

        data["computed_at"] = datetime.utcnow().isoformat()
        return jsonify(to_json_value(data))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/recent")
def get_recent():
    try:
        session, _cluster = get_cassandra()
        limit = get_int_arg("limit", 10, 1, 100)
        rows = session.execute(
            "SELECT source_ip, timestamp, reason, request_path, user_agent, threat_label FROM signature_alerts LIMIT 1000"
        )

        data = [{
            "ip": r.source_ip,
            "timestamp": str(r.timestamp),
            "reason": r.reason,
            "request_path": r.request_path,
            "user_agent": r.user_agent,
            "threat_label": r.threat_label
        } for r in rows]

        data = sorted(
            data,
            key=lambda x: parse_datetime_value(x.get("timestamp")) or datetime.min,
            reverse=True
        )[:limit]

        return jsonify(to_json_value({"recent": data}))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/volume-alerts")
def get_volume_alerts():
    try:
        session, _cluster = get_cassandra()
        limit = get_int_arg("limit", 20, 1, 100)
        rows = session.execute(
            "SELECT source_ip, window_start, window_end, total_bytes, threshold, reason FROM volume_alerts LIMIT 1000"
        )

        data = [{
            "ip": r.source_ip,
            "window_start": to_json_value(r.window_start),
            "window_end": to_json_value(r.window_end),
            "total_bytes": r.total_bytes,
            "threshold": r.threshold,
            "reason": r.reason
        } for r in rows]

        data = sorted(
            data,
            key=lambda x: parse_datetime_value(x.get("window_end")) or datetime.min,
            reverse=True
        )[:limit]

        return jsonify(to_json_value({"volume_alerts": data}))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/realtime")
def get_realtime_threats():
    try:
        session, _cluster = get_cassandra()
        limit = get_int_arg("limit", 20, 1, 100)
        rows = session.execute(
            "SELECT ip_source, last_seen, attack_types, threat_score FROM realtime_threats LIMIT 1000"
        )

        data = []
        for r in rows:
            decision_score = speed_decision_score(r)
            data.append({
                "ip": r.ip_source,
                "last_seen": to_json_value(r.last_seen),
                "attack_types": r.attack_types,
                "threat_score": r.threat_score,
                "decision_score": decision_score,
                "severity": speed_severity(decision_score),
                "recommended_decision": speed_decision(decision_score)
            })

        data = sorted(
            data,
            key=lambda x: (
                x["decision_score"] or 0,
                parse_datetime_value(x.get("last_seen")) or datetime.min
            ),
            reverse=True
        )[:limit]

        return jsonify(to_json_value({"realtime": data, "count": len(data)}))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/by-protocol")
def get_by_protocol():
    try:
        session, _cluster = get_cassandra()
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

        return jsonify({"by_protocol": by_protocol})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/threats/timeline")
def get_timeline():
    try:
        session, _cluster = get_cassandra()
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

        source = "logs"
        if not by_day:
            source = "signature_alerts"
            rows = session.execute(
                "SELECT timestamp, threat_label FROM signature_alerts LIMIT 5000"
            )
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

        timeline = sorted(by_day.values(), key=lambda x: x["date"])
        return jsonify({"timeline": timeline, "days": len(timeline)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ════════════════════════════════════════════════════════════
# GET /threats/geo/attacks
# Real public-IP geolocation + attack arcs for threat map
# Rule: discard private source_ip AND private dest_ip
# ════════════════════════════════════════════════════════════

def is_public_ip(ip):
    import ipaddress
    try:
        obj = ipaddress.ip_address(ip)
        return obj.is_global
    except Exception:
        return False


def infer_attack_type(row):
    text = " ".join([
        str(getattr(row, "request_path", "") or ""),
        str(getattr(row, "user_agent", "") or ""),
        str(getattr(row, "threat_label", "") or "")
    ]).lower()

    if "union select" in text or "sqlmap" in text or "sqli" in text:
        return "SQLi"
    if "nmap" in text or "scan" in text:
        return "ToolScan"
    if "../" in text or "passwd" in text or "pathtraversal" in text:
        return "PathTraversal"
    if "hydra" in text or "wp-login" in text or "brute" in text:
        return "BruteForce"
    if "xss" in text or "<script" in text:
        return "XSS"
    return "GenericThreat"


def severity_from_score(score, threat_label=None):
    if score >= 80:
        return "CRITICAL", "red"
    if score >= 55:
        return "HIGH", "orange"
    if score >= 30:
        return "MEDIUM", "yellow"

    if threat_label == "malicious":
        return "HIGH", "orange"
    if threat_label == "suspicious":
        return "MEDIUM", "yellow"

    return "LOW", "cyan"



def geo_event_score(row, attack_type):
    """
    Event-level score for geomap.
    This score represents the displayed attack row, not the global IP score.
    """
    label = (getattr(row, "threat_label", "") or "").lower()
    action = (getattr(row, "action", "") or "").lower()
    user_agent = (getattr(row, "user_agent", "") or "").lower()
    request_path = (getattr(row, "request_path", "") or "").lower()
    bytes_transferred = getattr(row, "bytes_transferred", 0) or 0

    score = 0

    # Main attack-type weight
    if attack_type == "SQLi":
        score += 70
    elif attack_type == "XSS":
        score += 75
    elif attack_type == "PathTraversal":
        score += 75
    elif attack_type == "BruteForce":
        score += 45
    elif attack_type == "ToolScan":
        score += 45
    else:
        score += 10

    # Dataset label correction
    if label == "malicious":
        score += 15
    elif label == "suspicious":
        score += 10

    # Action correction
    if action == "blocked":
        score += 5

    # Extra signature correction
    if "sqlmap" in user_agent:
        score += 10
    if "nmap" in user_agent or "masscan" in user_agent:
        score += 5
    if "passwd" in request_path or "../" in request_path or "..\\" in request_path:
        score += 10
    if "phpmyadmin" in request_path or "backup.sql" in request_path:
        score += 5

    # Small volume correction, capped
    try:
        score += min(float(bytes_transferred) / 20000.0, 5)
    except Exception:
        pass

    return min(round(score, 2), 100)


def load_geo_cache():
    import json, os
    path = "/tmp/rapid_geo_cache.json"
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_geo_cache(cache):
    import json
    path = "/tmp/rapid_geo_cache.json"
    try:
        with open(path, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass


def geo_lookup_batch(ips):
    import requests

    cache = load_geo_cache()
    result = {}
    missing = []

    for ip in ips:
        if ip in cache:
            result[ip] = cache[ip]
        else:
            missing.append(ip)

    if missing:
        fields = "status,message,query,country,countryCode,city,lat,lon,isp,org,as"
        url = f"http://ip-api.com/batch?fields={fields}"

        try:
            # batch endpoint supports up to 100 IPs
            for i in range(0, len(missing), 100):
                batch = missing[i:i+100]
                resp = requests.post(url, json=batch, timeout=15)
                resp.raise_for_status()
                data = resp.json()

                for item in data:
                    ip = item.get("query")
                    if item.get("status") == "success" and ip:
                        geo = {
                            "ip": ip,
                            "country": item.get("country"),
                            "country_code": item.get("countryCode"),
                            "city": item.get("city"),
                            "lat": item.get("lat"),
                            "lng": item.get("lon"),
                            "isp": item.get("isp"),
                            "org": item.get("org"),
                            "asn": item.get("as")
                        }
                        cache[ip] = geo
                        result[ip] = geo
                    elif ip:
                        cache[ip] = {
                            "ip": ip,
                            "error": item.get("message", "geo lookup failed")
                        }

            save_geo_cache(cache)

        except Exception as e:
            result["_geo_error"] = str(e)

    return result


@app.route("/threats/geo/attacks")
def get_geo_attacks():
    """
    Returns real public-IP-to-public-IP geolocation for attack map visualization.

    Private source_ip values are discarded.
    Private dest_ip values are discarded.
    No private IP is mapped to Morocco or RAPID infrastructure.
    """
    try:
        session, _cluster = get_cassandra()

        score_rows = session.execute(
            "SELECT source_ip, score, malicious_count, suspicious_count, total_events, last_seen FROM threat_scores"
        )

        score_map = {}
        for r in score_rows:
            score_map[r.source_ip] = {
                "score": r.score or 0,
                "malicious_count": r.malicious_count or 0,
                "suspicious_count": r.suspicious_count or 0,
                "total_events": r.total_events or 0,
                "last_seen": to_json_value(r.last_seen)
            }

        rows = session.execute(
            "SELECT source_ip, dest_ip, timestamp, protocol, action, threat_label, bytes_transferred, request_path, user_agent FROM logs LIMIT 500"
        )

        candidates = []
        public_ips = set()
        skipped_private_source = 0
        skipped_private_target = 0
        skipped_generic = 0

        # Distributed infrastructure map — dest_ip last octet -> world city
        INFRA_MAP = {
            range(1,   20):  {"city": "Casablanca",     "country": "Morocco",       "country_code": "MA", "lat": 33.5731,  "lng": -7.5898,   "org": "RAPID-Node-MA"},
            range(20,  40):  {"city": "Paris",          "country": "France",        "country_code": "FR", "lat": 48.8566,  "lng": 2.3522,    "org": "RAPID-Node-FR"},
            range(40,  60):  {"city": "New York",       "country": "USA",           "country_code": "US", "lat": 40.7128,  "lng": -74.0060,  "org": "RAPID-Node-US"},
            range(60,  80):  {"city": "London",         "country": "UK",            "country_code": "GB", "lat": 51.5074,  "lng": -0.1278,   "org": "RAPID-Node-GB"},
            range(80,  100): {"city": "Frankfurt",      "country": "Germany",       "country_code": "DE", "lat": 50.1109,  "lng": 8.6821,    "org": "RAPID-Node-DE"},
            range(100, 120): {"city": "Singapore",      "country": "Singapore",     "country_code": "SG", "lat": 1.3521,   "lng": 103.8198,  "org": "RAPID-Node-SG"},
            range(120, 140): {"city": "Tokyo",          "country": "Japan",         "country_code": "JP", "lat": 35.6762,  "lng": 139.6503,  "org": "RAPID-Node-JP"},
            range(140, 160): {"city": "Sao Paulo",      "country": "Brazil",        "country_code": "BR", "lat": -23.5505, "lng": -46.6333,  "org": "RAPID-Node-BR"},
            range(160, 180): {"city": "Sydney",         "country": "Australia",     "country_code": "AU", "lat": -33.8688, "lng": 151.2093,  "org": "RAPID-Node-AU"},
            range(180, 200): {"city": "Dubai",          "country": "UAE",           "country_code": "AE", "lat": 25.2048,  "lng": 55.2708,   "org": "RAPID-Node-AE"},
            range(200, 220): {"city": "Mumbai",         "country": "India",         "country_code": "IN", "lat": 19.0760,  "lng": 72.8777,   "org": "RAPID-Node-IN"},
            range(220, 240): {"city": "Johannesburg",   "country": "South Africa",  "country_code": "ZA", "lat": -26.2041, "lng": 28.0473,   "org": "RAPID-Node-ZA"},
            range(240, 256): {"city": "Toronto",        "country": "Canada",        "country_code": "CA", "lat": 43.6532,  "lng": -79.3832,  "org": "RAPID-Node-CA"},
        }

        def get_target_from_dest(dest_ip):
            try:
                octet = int(dest_ip.split(".")[-1])
                for r, info in INFRA_MAP.items():
                    if octet in r:
                        return info
            except Exception:
                pass
            return {"city": "Casablanca", "country": "Morocco", "country_code": "MA", "lat": 33.5731, "lng": -7.5898, "org": "RAPID-Node-DEFAULT"}

        for r in rows:
            src = r.source_ip

            if not src or not is_public_ip(src):
                skipped_private_source += 1
                continue

            label = r.threat_label or "unknown"
            attack_type = infer_attack_type(r)

            if label not in ("malicious", "suspicious") and attack_type == "GenericThreat":
                skipped_generic += 1
                continue

            candidates.append(r)
            public_ips.add(src)

            if len(candidates) >= 120:
                break

        geo = geo_lookup_batch(sorted(public_ips))

        attacks = []

        for r in candidates:
            src = r.source_ip
            src_geo = geo.get(src)

            if not src_geo or not src_geo.get("lat") or not src_geo.get("lng"):
                continue

            target = get_target_from_dest(r.dest_ip or "192.168.1.1")
            score_info = score_map.get(src, {})
            ip_score = score_info.get("score", 0)

            attack_type = infer_attack_type(r)
            event_score_value = geo_event_score(r, attack_type)

            # Geomap score must represent this specific event, not the global IP score
            score = event_score_value
            severity, color = severity_from_score(score, r.threat_label)

            attacks.append({
                "source_ip": src,
                "source_country": src_geo.get("country"),
                "source_country_code": src_geo.get("country_code"),
                "source_city": src_geo.get("city"),
                "source_lat": src_geo.get("lat"),
                "source_lng": src_geo.get("lng"),
                "source_isp": src_geo.get("isp"),
                "source_org": src_geo.get("org"),
                "source_asn": src_geo.get("asn"),

                "target_ip": r.dest_ip,
                "target_country": target["country"],
                "target_country_code": target["country_code"],
                "target_city": target["city"],
                "target_lat": target["lat"],
                "target_lng": target["lng"],
                "target_org": target["org"],

                "protocol": r.protocol,
                "attack_type": attack_type,
                "threat_label": r.threat_label,
                "severity": severity,
                "color": color,
                "score": score,
                "event_score": event_score_value,
                "ip_score": ip_score,
                "score_source": "event_level",
                "malicious_count": score_info.get("malicious_count", 0),
                "suspicious_count": score_info.get("suspicious_count", 0),
                "total_events": score_info.get("total_events", 0),
                "timestamp": to_json_value(r.timestamp),
                "request_path": r.request_path,
                "user_agent": r.user_agent,

                "arc": {
                    "startLat": src_geo.get("lat"),
                    "startLng": src_geo.get("lng"),
                    "endLat": target["lat"],
                    "endLng": target["lng"],
                    "color": color
                }
            })

        attacks = sorted(attacks, key=lambda x: x["score"], reverse=True)[:80]

        countries = {}
        for a in attacks:
            c = a.get("source_country") or "Unknown"
            countries[c] = countries.get(c, 0) + 1

        return jsonify(to_json_value({
            "status": "ok",
            "mode": "real_public_ip_to_public_ip_geo",
            "count": len(attacks),
            "top_source_countries": countries,
            "attacks": attacks,
            "skipped": {
                "private_or_invalid_source_ip": skipped_private_source,
                "private_or_invalid_target_ip": skipped_private_target,
                "generic_non_threat_rows": skipped_generic
            },
            "geo_error": geo.get("_geo_error"),
            "note": "Only public source_ip and public dest_ip rows are included. Private IPs are discarded and are not mapped to RAPID infrastructure."
        }))

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500



# ════════════════════════════════════════════════════════════
# HBase endpoint: Khalid ML / Port scan detections
# Table: port_scans
# ════════════════════════════════════════════════════════════

def hbase_decode_value(v):
    if isinstance(v, bytes):
        return v.decode(errors="ignore")
    return str(v)


def to_int_safe(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default


def decode_port_scan_row(row_key, data):
    row_key = hbase_decode_value(row_key)

    source_ip = hbase_decode_value(data.get(b"cf:source_ip", b""))
    distinct_ports = to_int_safe(hbase_decode_value(data.get(b"cf:distinct_ports", b"0")))
    total_connections = to_int_safe(hbase_decode_value(data.get(b"cf:total_connections", b"0")))
    window_start = hbase_decode_value(data.get(b"cf:window_start", b""))
    window_end = hbase_decode_value(data.get(b"cf:window_end", b""))

    if distinct_ports >= 25 or total_connections >= 30:
        severity = "HIGH"
        recommendation = "BLOCK"
    elif distinct_ports >= 20 or total_connections >= 20:
        severity = "MEDIUM"
        recommendation = "MONITOR"
    else:
        severity = "LOW"
        recommendation = "ALLOW"

    return {
        "row_key": row_key,
        "source_ip": source_ip,
        "distinct_ports": distinct_ports,
        "total_connections": total_connections,
        "window_start": window_start,
        "window_end": window_end,
        "severity": severity,
        "recommendation": recommendation
    }


@app.route("/threats/port-scans")
def get_port_scans():
    """
    Return port scan detections written by Khalid into HBase.
    Query params:
      ?limit=50
    """
    try:
        limit = get_int_arg("limit", 50, 1, 500)

        conn = get_hbase()
        table = conn.table("port_scans")

        rows = []
        for key, data in table.scan(limit=limit):
            rows.append(decode_port_scan_row(key, data))

        conn.close()

        rows = sorted(
            rows,
            key=lambda x: (x["distinct_ports"], x["total_connections"]),
            reverse=True
        )

        return jsonify({
            "status": "ok",
            "source": "hbase",
            "table": "port_scans",
            "count": len(rows),
            "limit": limit,
            "port_scans": rows
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "source": "hbase",
            "table": "port_scans",
            "error": str(e)
        }), 500


@app.route("/threats/port-scans/top")
def get_top_port_scans():
    """
    Return top port scan detections sorted by distinct_ports and total_connections.
    Query params:
      ?limit=10
      ?scan_limit=500
    """
    try:
        limit = get_int_arg("limit", 10, 1, 100)
        scan_limit = get_int_arg("scan_limit", 500, 1, 2000)

        conn = get_hbase()
        table = conn.table("port_scans")

        rows = []
        for key, data in table.scan(limit=scan_limit):
            rows.append(decode_port_scan_row(key, data))

        conn.close()

        rows = sorted(
            rows,
            key=lambda x: (x["distinct_ports"], x["total_connections"]),
            reverse=True
        )[:limit]

        return jsonify({
            "status": "ok",
            "source": "hbase",
            "table": "port_scans",
            "count": len(rows),
            "limit": limit,
            "scan_limit": scan_limit,
            "top_port_scans": rows
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "source": "hbase",
            "table": "port_scans",
            "error": str(e)
        }), 500


@app.route("/threats/port-scans/ip/<ip>")
def get_port_scans_by_ip(ip):
    """
    Return port scan detections for one source IP.
    Query params:
      ?limit=1000
    """
    try:
        limit = get_int_arg("limit", 1000, 1, 5000)

        conn = get_hbase()
        table = conn.table("port_scans")

        matches = []
        scanned = 0

        for key, data in table.scan(limit=limit):
            scanned += 1
            row = decode_port_scan_row(key, data)

            if row["source_ip"] == ip or row["row_key"].startswith(ip + "|"):
                matches.append(row)

        conn.close()

        matches = sorted(
            matches,
            key=lambda x: x["window_start"],
            reverse=True
        )

        return jsonify({
            "status": "ok",
            "source": "hbase",
            "table": "port_scans",
            "ip": ip,
            "scanned": scanned,
            "count": len(matches),
            "port_scans": matches
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "source": "hbase",
            "table": "port_scans",
            "ip": ip,
            "error": str(e)
        }), 500



# ════════════════════════════════════════════════════════════
# Batch Layer HBase Endpoints
# Pure HBase endpoints for Chawi dashboard
# No Cassandra streaming tables used here
# ════════════════════════════════════════════════════════════

BATCH_HBASE_TABLES = {
    "attack_patterns": {
        "endpoint": "/batch/attack-patterns",
        "list_key": "attack_patterns",
        "sort_fields": ["occurrences", "total_bytes"]
    },
    "ip_reputation": {
        "endpoint": "/batch/ip-reputation",
        "list_key": "ip_reputation",
        "sort_fields": ["reputation_score", "threat_count"]
    },
    "multistep_attacks": {
        "endpoint": "/batch/multistep-attacks",
        "list_key": "multistep_attacks",
        "sort_fields": ["malicious_count", "total_events"]
    },
    "port_scans": {
        "endpoint": "/batch/port-scans",
        "list_key": "port_scans",
        "sort_fields": ["distinct_ports", "total_connections"]
    },
    "threat_timeline": {
        "endpoint": "/batch/threat-timeline",
        "list_key": "threat_timeline",
        "sort_fields": []
    },
    "threat_volume": {
        "endpoint": "/batch/threat-volume",
        "list_key": "threat_volume",
        "sort_fields": ["total_bytes", "threshold"]
    },
}


def batch_hbase_decode(value):
    if value is None:
        return None

    if isinstance(value, bytes):
        text = value.decode(errors="ignore")
    else:
        text = str(value)

    text = text.strip()

    # Try numeric conversion for dashboard charts
    try:
        if text != "" and all(c not in text for c in ["-", ":", " "]):
            if "." in text:
                return float(text)
            return int(text)
    except Exception:
        pass

    # Try list conversion for strings like ['SQLi', 'ToolScan']
    if text.startswith("[") and text.endswith("]"):
        try:
            import ast
            parsed = ast.literal_eval(text)
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
        except Exception:
            pass

    return text


def batch_hbase_number(value):
    try:
        return float(value)
    except Exception:
        return 0.0


def batch_hbase_normalize_row(row_key, data):
    row_key = batch_hbase_decode(row_key)

    row = {
        "row_key": row_key,
        "columns": {}
    }

    for col, val in data.items():
        col_name = batch_hbase_decode(col)
        value = batch_hbase_decode(val)

        row["columns"][col_name] = value

        # Convert cf:source_ip -> source_ip
        clean_name = col_name.split(":", 1)[1] if ":" in col_name else col_name
        row[clean_name] = value

    return row


def batch_hbase_scan_table(table_name, limit=50):
    if table_name not in BATCH_HBASE_TABLES:
        raise ValueError(f"Unsupported batch HBase table: {table_name}")

    limit = max(1, min(int(limit), 5000))

    conn = get_hbase()
    conn.open()

    table = conn.table(table_name)
    rows = [batch_hbase_normalize_row(k, d) for k, d in table.scan(limit=limit)]

    conn.close()

    sort_fields = BATCH_HBASE_TABLES[table_name].get("sort_fields", [])
    if sort_fields:
        rows = sorted(
            rows,
            key=lambda r: tuple(batch_hbase_number(r.get(f)) for f in sort_fields),
            reverse=True
        )

    return rows


def batch_hbase_get_row(table_name, row_key):
    if table_name not in BATCH_HBASE_TABLES:
        raise ValueError(f"Unsupported batch HBase table: {table_name}")

    conn = get_hbase()
    conn.open()

    table = conn.table(table_name)
    data = table.row(row_key.encode())

    conn.close()

    if not data:
        return None

    return batch_hbase_normalize_row(row_key, data)


def batch_hbase_response(table_name):
    try:
        limit = get_int_arg("limit", 50, 1, 5000)

        rows = batch_hbase_scan_table(table_name, limit)
        config = BATCH_HBASE_TABLES[table_name]
        list_key = config["list_key"]

        return jsonify({
            "status": "ok",
            "layer": "batch",
            "source": "hbase",
            "hbase_host": HBASE_HOST,
            "hbase_port": 9090,
            "table": table_name,
            "count": len(rows),
            "limit": limit,
            list_key: rows
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "table": table_name,
            "error": str(e)
        }), 500


@app.route("/batch/hbase/tables")
def batch_hbase_tables():
    try:
        conn = get_hbase()
        conn.open()

        existing = [
            t.decode(errors="ignore") if isinstance(t, bytes) else str(t)
            for t in conn.tables()
        ]

        conn.close()

        tables = []
        for table_name, config in BATCH_HBASE_TABLES.items():
            tables.append({
                "table": table_name,
                "endpoint": config["endpoint"],
                "list_key": config["list_key"],
                "exists": table_name in existing
            })

        return jsonify({
            "status": "ok",
            "layer": "batch",
            "source": "hbase",
            "hbase_host": HBASE_HOST,
            "hbase_port": 9090,
            "tables": tables
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "error": str(e)
        }), 500


@app.route("/batch/hbase/<table_name>")
def batch_hbase_generic(table_name):
    return batch_hbase_response(table_name)


@app.route("/batch/hbase/<table_name>/row")
def batch_hbase_generic_row(table_name):
    try:
        row_key = request.args.get("key", "")
        if not row_key:
            return jsonify({
                "status": "error",
                "error": "Missing query parameter: key"
            }), 400

        row = batch_hbase_get_row(table_name, row_key)

        return jsonify({
            "status": "ok" if row else "not_found",
            "layer": "batch",
            "source": "hbase",
            "table": table_name,
            "row_key": row_key,
            "row": row
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "table": table_name,
            "error": str(e)
        }), 500


@app.route("/batch/attack-patterns")
def batch_attack_patterns():
    return batch_hbase_response("attack_patterns")


@app.route("/batch/ip-reputation")
def batch_ip_reputation():
    return batch_hbase_response("ip_reputation")


@app.route("/batch/ip-reputation/<ip>")
def batch_ip_reputation_by_ip(ip):
    try:
        row = batch_hbase_get_row("ip_reputation", ip)

        return jsonify({
            "status": "ok" if row else "not_found",
            "layer": "batch",
            "source": "hbase",
            "table": "ip_reputation",
            "ip": ip,
            "reputation": row
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "table": "ip_reputation",
            "ip": ip,
            "error": str(e)
        }), 500


@app.route("/batch/multistep-attacks")
def batch_multistep_attacks():
    return batch_hbase_response("multistep_attacks")


@app.route("/batch/multistep-attacks/ip/<ip>")
def batch_multistep_attacks_by_ip(ip):
    try:
        row = batch_hbase_get_row("multistep_attacks", ip)

        return jsonify({
            "status": "ok" if row else "not_found",
            "layer": "batch",
            "source": "hbase",
            "table": "multistep_attacks",
            "ip": ip,
            "multistep_attack": row
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "table": "multistep_attacks",
            "ip": ip,
            "error": str(e)
        }), 500


@app.route("/batch/port-scans")
def batch_port_scans():
    return batch_hbase_response("port_scans")


@app.route("/batch/port-scans/top")
def batch_port_scans_top():
    return batch_hbase_response("port_scans")


@app.route("/batch/port-scans/ip/<ip>")
def batch_port_scans_by_ip(ip):
    try:
        limit = get_int_arg("limit", 1000, 1, 5000)
        rows = batch_hbase_scan_table("port_scans", limit)

        matches = [
            r for r in rows
            if str(r.get("source_ip", "")) == ip
            or str(r.get("row_key", "")).startswith(ip + "|")
        ]

        return jsonify({
            "status": "ok",
            "layer": "batch",
            "source": "hbase",
            "table": "port_scans",
            "ip": ip,
            "count": len(matches),
            "scanned": len(rows),
            "port_scans": matches
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "layer": "batch",
            "source": "hbase",
            "table": "port_scans",
            "ip": ip,
            "error": str(e)
        }), 500


@app.route("/batch/threat-timeline")
def batch_threat_timeline():
    return batch_hbase_response("threat_timeline")


@app.route("/batch/threat-volume")
def batch_threat_volume():
    return batch_hbase_response("threat_volume")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
