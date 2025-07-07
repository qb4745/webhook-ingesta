import os
import json
import functions_framework
from google.cloud import pubsub_v1

# --- Configuración ---
# Se obtienen las variables del entorno del servicio de Cloud Run.
# Esta es una mejor práctica que evita hardcodear valores en el código.
PROJECT_ID = os.environ.get('GCP_PROJECT')
TOPIC_ID = os.environ.get('TOPIC_ID')

# Se inicializa el cliente de Pub/Sub una sola vez para reutilizar la conexión
# y mejorar el rendimiento en invocaciones posteriores (cold starts).
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.http
def main(request):
    """
    Función HTTP que actúa como endpoint para un webhook.
    1. Maneja peticiones de verificación (handshake) con cuerpo vacío.
    2. Publica los payloads con datos en un tópico de Pub/Sub.
    3. Emite logs estructurados para observabilidad.
    """
    
    # Se extrae el cuerpo (payload) de la petición HTTP.
    payload = request.get_data()
    
    # --- Lógica de Manejo de Verificación (Handshake) ---
    # Si el payload está vacío, se asume que es una petición de verificación del webhook
    # para comprobar si el endpoint está activo.
    if not payload:
        # Se crea una entrada de log estructurada para registrar el evento.
        log_entry_verification = {
            "severity": "INFO",
            "message": "Petición de verificación (handshake) recibida con payload vacío. Respondiendo OK.",
            "source_ip": request.remote_addr,
            "user_agent": request.headers.get('User-Agent')
        }
        # Se imprime el JSON al stdout, que Cloud Logging captura automáticamente.
        print(json.dumps(log_entry_verification))
        
        # Se responde con 200 OK para confirmar al webhook que el endpoint está listo.
        return ("Endpoint verificado y listo para recibir datos.", 200)

    # --- Lógica de Procesamiento de Datos ---
    # Si el payload contiene datos, se procede a publicarlo.
    try:
        # Se publica el payload (que ya está en formato de bytes) en el tópico de Pub/Sub.
        future = publisher.publish(topic_path, payload)
        message_id = future.result()  # El .result() confirma que la publicación fue exitosa.

        # Se crea un log estructurado de éxito.
        log_entry_info = {
            "severity": "INFO",
            "message": "Mensaje de datos recibido y publicado en Pub/Sub exitosamente",
            "pubsub_message_id": message_id,
            "topic": topic_path,
            "payload_size_bytes": len(payload)
        }
        print(json.dumps(log_entry_info))
        
        # Se responde con 200 OK para notificar a la fuente que el mensaje fue aceptado.
        return ("Mensaje recibido y encolado para procesamiento.", 200)

    except Exception as e:
        # Si ocurre cualquier error durante la publicación en Pub/Sub.
        # Se crea un log estructurado de error, incluyendo el payload para facilitar la depuración.
        log_entry_error = {
            "severity": "ERROR",
            "message": f"Error al publicar el mensaje en Pub/Sub: {e}",
            "original_payload": payload.decode('utf-8', errors='ignore') # Se decodifica para que sea legible.
        }
        print(json.dumps(log_entry_error))
        
        # Se responde con 500 Internal Server Error para indicar un fallo en el servidor.
        return ("Error interno al procesar el mensaje.", 500)