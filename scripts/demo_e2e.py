# scripts/demo_e2e.py
import subprocess, time, requests, os

ANASS_IP = os.getenv("ANASS_IP", "100.73.216.115")
CHAWI_IP = os.getenv("CHAWI_IP", "100.97.208.110")

print("=== RAPID — Demo end-to-end Sprint 3 ===\n")

# Étape 1 — Lancer le producer Kafka
print("[1/4] Démarrage du producteur Kafka...")
producer = subprocess.Popen([
    "python3", "spark/speed_layer/kafka_producer.py"
])
time.sleep(5)

# Étape 2 — Vérifier Spark Streaming (Khalid)
print("[2/4] Kafka producer actif — Khalid doit voir les events dans spark-streaming")
time.sleep(10)

# Étape 3 — Tester l'API Flask d'Anass
print("[3/4] Test API Flask d'Anass...")
try:
    r = requests.get(f"http://{ANASS_IP}:5000/threats/top10", timeout=10)
    print(f"  /threats/top10    → HTTP {r.status_code}")
    if r.status_code == 200:
        print(f"  {r.json()}")

    r2 = requests.get(f"http://{ANASS_IP}:5000/threats/timeline", timeout=10)
    print(f"  /threats/timeline → HTTP {r2.status_code}")
    if r2.status_code == 200:
        print(f"  {r2.json()}")

except requests.exceptions.ConnectionError:
    print(f"  API non disponible sur {ANASS_IP}:5000 — attendre qu'Anass lance flask-api")

# Étape 4 — Dashboard Chawi
print(f"\n[4/4] Dashboard disponible sur : http://{CHAWI_IP}:3000")

producer.terminate()
print("\n=== Demo terminée ===")