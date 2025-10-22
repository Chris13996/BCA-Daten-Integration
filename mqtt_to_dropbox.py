import paho.mqtt.client as mqtt
import dropbox
import os
import json
import time
from datetime import datetime

# --- Konfiguration ---
MQTT_BROKER = "mqtt.flespi.io"
MQTT_PORT = 8883
MQTT_TOKEN = os.getenv("MQTT_TOKEN")  # GitHub Secret
MQTT_TOPIC = "bca/Test13/kennzeichen"
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")  # GitHub Secret
DROPBOX_FILE = f"/mqtt_daten/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

# --- Dropbox-Testfunktion ---
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

# --- Manueller Dropbox-Upload-Test ---
def manual_dropbox_upload():
    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        test_payload = '{"kennzeichen": "MANUELLER_TEST", "zeitstempel": "' + datetime.now().isoformat() + '"}'
        dbx.files_upload(
            test_payload.encode("utf-8"),
            "/manual_test.json",
            mode=dropbox.files.WriteMode.overwrite
        )
        print("✅ Manueller Dropbox-Upload erfolgreich!")
    except Exception as e:
        print(f"❌ Manueller Dropbox-Upload fehlgeschlagen: {e}")

# --- MQTT-Callbacks ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Verbunden mit MQTT-Broker (Topic: {MQTT_TOPIC})")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"❌ Verbindungsfehler: {mqtt.error_string(rc)}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"\n--- Neue MQTT-Nachricht empfangen ---")
    print(f"Topic: {msg.topic}")
    print(f"Payload: {payload}")

    # Speichere die Payload in einer temporären Datei für Debug-Zwecke
    with open("temp_payload.json", "w") as f:
        f.write(payload)

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
            print(f"✅ Datei erfolgreich nach Dropbox hochgeladen: {DROPBOX_FILE}")
            break
        except Exception as e:
            print(f"❌ Upload-Versuch {attempt + 1}/{max_retries} fehlgeschlagen: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)

# --- Hauptprogramm ---
if __name__ == "__main__":
    # 1. Dropbox-Verbindung testen
    if not test_dropbox_upload():
        print("🚨 Dropbox-Verbindung konnte nicht hergestellt werden. Skript wird beendet.")
        exit(1)

    # 2. Manuellen Dropbox-Upload testen
    manual_dropbox_upload()

    # 3. MQTT-Client initialisieren
    client = mqtt.Client()
    client.tls_set()
    client.username_pw_set(MQTT_TOKEN)
    client.on_connect = on_connect
    client.on_message = on_message

    # 4. Verbindung herstellen
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print("🔍 Warte auf MQTT-Nachrichten...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("Skript durch Nutzer beendet.")
    except Exception as e:
        print(f"❌ Kritischer Fehler: {e}")
    finally:
        client.disconnect()
        print("MQTT-Client getrennt.")
