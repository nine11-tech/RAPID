from flask import Flask, jsonify
from flask_cors import CORS
from cassandra.cluster import Cluster
import happybase
import os
import urllib.request
import urllib.parse
from datetime import datetime

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

def compute_adaptive_threshold(session):
    """
    Adaptive threshold based on current threat_scores table.
    In our project, this approximates the recent active scoring window
    produced by Spark Streaming.
    """
    rows = session.execute("SELECT score FROM threat_scores")
    scores = [r.score for r in rows if r.score is not None]

    if not scores:
        return {
            "threshold": 50,
            "avg_score": 0,
            "total_ips": 0,
            "formula": "default"
        }

    avg = sum(scores) / len(scores)
    threshold = min(avg * 1.5, 100)

    return {
        "threshold": round(threshold, 2),
        "avg_score": round(avg, 2),
        "total_ips": len(scores),
        "formula": "avg_score * 1.5 capped at 100"
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
        session, cluster = get_cassandra()
        threshold_info = compute_adaptive_threshold(session)
        cluster.shutdown()
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
        data = compute_adaptive_threshold(session)
        cluster.shutdown()

        data["computed_at"] = datetime.utcnow().isoformat()
        return jsonify(data)

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
        session, cluster = get_cassandra()

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
                "last_seen": r.last_seen
            }

        rows = session.execute(
            "SELECT source_ip, dest_ip, timestamp, protocol, threat_label, request_path, user_agent FROM logs LIMIT 500"
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
            score = score_info.get("score", 0)
            severity, color = severity_from_score(score, r.threat_label)
            attack_type = infer_attack_type(r)

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
                "malicious_count": score_info.get("malicious_count", 0),
                "suspicious_count": score_info.get("suspicious_count", 0),
                "total_events": score_info.get("total_events", 0),
                "timestamp": str(r.timestamp),
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

        cluster.shutdown()

        attacks = sorted(attacks, key=lambda x: x["score"], reverse=True)[:80]

        countries = {}
        for a in attacks:
            c = a.get("source_country") or "Unknown"
            countries[c] = countries.get(c, 0) + 1

        return jsonify({
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
        })

    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
