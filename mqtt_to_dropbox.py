import paho.mqtt.client as mqtt
import dropbox
import json
import os
from datetime import datetime

# MQTT-Konfiguration (flespi)
MQTT_BROKER = "mqtt.flespi.io"
MQTT_PORT = 8883
MQTT_TOKEN = os.getenv("MQTT_TOKEN")  # Wird in GitHub Secrets gespeichert
MQTT_TOPIC = "bca/Test13/kennzeichen"   # Produktiv-Topic

# Dropbox-Konfiguration
DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN")  # Wird in GitHub Secrets gespeichert
DROPBOX_FILE = f"/mqtt_daten_{datetime.now().strftime('%Y%m%d')}.json"

# Callback: Wird aufgerufen, wenn eine Nachricht empfangen wird
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"Nachricht erhalten: {payload}")

    try:
        # Daten in Dropbox hochladen
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        dbx.files_upload(
            payload.encode("utf-8"),
            DROPBOX_FILE,
            mode=dropbox.files.WriteMode.overwrite
        )
        print(f"Daten erfolgreich nach Dropbox hochgeladen: {DROPBOX_FILE}")
    except Exception as e:
        print(f"Fehler beim Hochladen nach Dropbox: {e}")

# MQTT-Client einrichten
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.tls_set()  # TLS-Verschlüsselung
client.username_pw_set(MQTT_TOKEN)
client.on_message = on_message

# Verbindung herstellen und Topic abonnieren
client.connect(MQTT_BROKER, MQTT_PORT)
client.subscribe(MQTT_TOPIC)
print(f"Abonniert: {MQTT_TOPIC}. Warte auf Nachrichten...")

# Endlos auf Nachrichten warten
client.loop_forever()
