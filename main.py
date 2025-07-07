import os
import json
import functions_framework
from google.cloud import pubsub_v1

# --- Variables de entorno ---
PROJECT_ID = os.environ.get('GCP_PROJECT')
TOPIC_ID = os.environ.get('TOPIC_ID')
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.http
def main(request):
    """
    Función HTTP que recibe un lote de registros, lo desagrega y publica cada
    registro como un mensaje individual en Pub/Sub de forma asíncrona y eficiente.
    """
    payload = request.get_data()

    # Verificación simple (usada por Google al crear la función)
    if not payload:
        print(json.dumps({
            "severity": "INFO",
            "message": "Petición de verificación recibida."
        }))
        return ("Endpoint verificado.", 200)

    try:
        data = json.loads(payload)
        publish_futures = []

        if isinstance(data, list):
            print(json.dumps({
                "severity": "INFO",
                "message": f"Lote de {len(data)} registros recibido. Preparando para publicación asíncrona..."
            }))

            for i, item in enumerate(data):
                if not isinstance(item, dict):
                    raise ValueError(f"Elemento en la posición {i} no es un objeto JSON válido: {item}")

                message_bytes = json.dumps(item).encode('utf-8')
                future = publisher.publish(topic_path, message_bytes)
                publish_futures.append(future)

        else:
            # Validar también que sea un dict (no un string, int, etc.)
            if not isinstance(data, dict):
                raise ValueError("El payload individual no es un objeto JSON válido.")

            message_bytes = json.dumps(data).encode('utf-8')
            future = publisher.publish(topic_path, message_bytes)
            publish_futures.append(future)

        # Esperar a que todas las publicaciones finalicen
        for future in publish_futures:
            future.result()

        success_message = f"{len(publish_futures)} mensajes publicados exitosamente en Pub/Sub."
        print(json.dumps({
            "severity": "INFO",
            "message": success_message
        }))
        return (success_message, 200)

    except Exception as e:
        log_entry_error = {
            "severity": "ERROR",
            "message": f"Fallo en el procesamiento del lote: {str(e)}",
            "original_payload": payload.decode('utf-8', errors='ignore')
        }
        print(json.dumps(log_entry_error))
        return ("Error interno al procesar el lote.", 500)
