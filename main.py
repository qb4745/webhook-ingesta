import os
import json
import functions_framework
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get('GCP_PROJECT')
TOPIC_ID = os.environ.get('TOPIC_ID')
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.http
def main(request):
    """
    Función HTTP que recibe un webhook, que puede contener un lote de registros.
    Desagrega el lote y publica cada registro como un mensaje individual en Pub/Sub.
    """
    payload = request.get_data()
    
    if not payload:
        # Maneja la verificación del webhook como antes
        print(json.dumps({"severity": "INFO", "message": "Petición de verificación recibida."}))
        return ("Endpoint verificado.", 200)

    try:
        # Decodifica el payload y lo carga como un objeto Python (puede ser una lista o un dict)
        data = json.loads(payload)
        
        messages_published = 0
        
        # --- LÓGICA DE DESAGREGACIÓN ---
        # Comprueba si los datos recibidos son una lista (un lote de registros)
        if isinstance(data, list):
            print(json.dumps({"severity": "INFO", "message": f"Lote de {len(data)} registros recibido. Desagregando..."}))
            # Itera sobre cada registro en la lista
            for item in data:
                # Convierte cada registro individual a un string JSON y luego a bytes
                message_bytes = json.dumps(item).encode('utf-8')
                # Publica el registro individual como un mensaje separado
                publisher.publish(topic_path, message_bytes).result()
                messages_published += 1
        else:
            # Si no es una lista, es un único registro. Lo publicamos directamente.
            print(json.dumps({"severity": "INFO", "message": "Registro único recibido. Publicando..."}))
            publisher.publish(topic_path, payload).result()
            messages_published = 1

        success_message = f"{messages_published} mensajes publicados exitosamente en Pub/Sub."
        print(json.dumps({"severity": "INFO", "message": success_message}))
        return (success_message, 200)

    except Exception as e:
        log_entry_error = {
            "severity": "ERROR",
            "message": f"Fallo en el procesamiento del lote: {e}",
            "original_payload": payload.decode('utf-8', errors='ignore')
        }
        print(json.dumps(log_entry_error))
        return ("Error interno al procesar el lote.", 500)