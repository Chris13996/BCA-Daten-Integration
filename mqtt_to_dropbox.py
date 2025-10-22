import paho.mqtt.client as mqtt
import dropbox
import os
import json
import time
from datetime import datetime

# --- Konfiguration ---
# MQTT (flespi)
MQTT_BROKER = "mqtt.flespi.io"
MQTT_PORT = 8883
MQTT_TOKEN = os.getenv("MQTT_TOKEN")  # GitHub Secret
MQTT_TOPIC = "bca/PROD/kennzeichen"

# Dropbox
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")  # GitHub Secret
DROPBOX_FILE = f"/mqtt_daten/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"  # Unterordner + Zeitstempel

# --- Dropbox-Testfunktion (vor MQTT-Client) ---
def test_dropbox_upload():
    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        dbx.files_upload(
            b"Test-Daten: Dropbox-Verbindung erfolgreich!",
            "/test_upload.txt",
            mode=dropbox.files.WriteMode.overwrite
        )
        print("✅ Dropbox-Testupload erfolgreich!")
        return True
    except Exception as e:
        print(f"❌ Dropbox-Testupload fehlgeschlagen: {e}")
        return False

# --- MQTT-Callbacks ---
def on_connect(client, userdata, flags, rc):
    print(f"Verbunden mit MQTT-Broker (Code: {rc})")
    if rc == 0:
        print(f"Abonniere Topic: {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Verbindungsfehler: {mqtt.connack_string(rc)}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"\n--- Neue Nachricht ---")
    print(f"Topic: {msg.topic}")
    print(f"Payload: {payload}")

    # Dropbox-Upload mit Wiederholungslogik
    max_retries = 3
    for attempt in range(max_retries):
        try:
            dbx = dropbox.Dropbox(DROPBOX_TOKEN)
            dbx.files_upload(
                payload.encode("utf-8"),
                DROPBOX_FILE,
                mode=dropbox.files.WriteMode.overwrite
            )
            print(f"✅ Datei hochgeladen: {DROPBOX_FILE}")
            break
        except Exception as e:
            print(f"❌ Upload-Versuch {attempt + 1}/{max_retries} fehlgeschlagen: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # Warte vor dem nächsten Versuch

# --- Hauptprogramm ---
if __name__ == "__main__":
    # 1. Dropbox-Verbindung testen
    if not test_dropbox_upload():
        print("🚨 Dropbox-Verbindung konnte nicht hergestellt werden. Skript wird beendet.")
        exit(1)

    # 2. MQTT-Client initialisieren (mit aktueller Callback-API)
    client = mqtt.Client()
    client.tls_set()  # TLS-Verschlüsselung
    client.username_pw_set(MQTT_TOKEN)
    client.on_connect = on_connect
    client.on_message = on_message

    # 3. Verbindung herstellen
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print("Starte MQTT-Client...")
        client.loop_forever()  # Blockierend warten
    except KeyboardInterrupt:
        print("Skript durch Nutzer beendet.")
    except Exception as e:
        print(f"❌ Kritischer Fehler: {e}")
    finally:
        client.disconnect()
        print("MQTT-Client getrennt.")
