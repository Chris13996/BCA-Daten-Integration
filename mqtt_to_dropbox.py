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
DROPBOX_FOLDER = "/mqtt_daten/"  # Zielordner in Dropbox

# --- MQTT-Callbacks (kompatibel mit allen Versionen) ---
def on_connect(client, userdata, flags, rc, properties=None):  # `properties` für neue Versionen
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

    # Dateinamen mit Zeitstempel generieren
    file_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    dropbox_path = DROPBOX_FOLDER + file_name

    # Dropbox-Upload mit Wiederholungslogik
    max_retries = 3
    for attempt in range(max_retries):
        try:
            dbx = dropbox.Dropbox(DROPBOX_TOKEN)
            dbx.files_upload(
                payload.encode("utf-8"),
                dropbox_path,
                mode=dropbox.files.WriteMode.overwrite
            )
            print(f"✅ Datei erfolgreich nach Dropbox hochgeladen: {dropbox_path}")
            break
        except Exception as e:
            print(f"❌ Upload-Versuch {attempt + 1}/{max_retries} fehlgeschlagen: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)

# --- Hauptprogramm ---
if __name__ == "__main__":
    # MQTT-Client initialisieren
    client = mqtt.Client()
    client.tls_set()
    client.username_pw_set(MQTT_TOKEN)
    client.on_connect = on_connect
    client.on_message = on_message

    # Verbindung herstellen
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        print("🔍 Warte auf MQTT-Nachrichten...")
        client.loop_forever()  # Kontinuierlich auf Nachrichten warten
    except KeyboardInterrupt:
        print("Skript durch Nutzer beendet.")
    except Exception as e:
        print(f"❌ Kritischer Fehler: {e}")
    finally:
        client.disconnect()
        print("MQTT-Client getrennt.")
