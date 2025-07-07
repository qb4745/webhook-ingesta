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
    Función HTTP que recibe un lote de registros, lo desagrega y publica cada
    registro como un mensaje individual en Pub/Sub de forma asíncrona y eficiente.
    """
    payload = request.get_data()
    
    if not payload:
        print(json.dumps({"severity": "INFO", "message": "Petición de verificación recibida."}))
        return ("Endpoint verificado.", 200)

    try:
        data = json.loads(payload)
        publish_futures = [] # Una lista para guardar los "futuros" de cada publicación
        
        if isinstance(data, list):
            print(json.dumps({"severity": "INFO", "message": f"Lote de {len(data)} registros recibido. Preparando para publicación asíncrona..."}))
            for item in data:
                message_bytes = json.dumps(item).encode('utf-8')
                # --- LÓGICA CORREGIDA ---
                # Disparamos la publicación pero NO esperamos aquí.
                # Guardamos el objeto 'future' en nuestra lista.
                future = publisher.publish(topic_path, message_bytes)
                publish_futures.append(future)
        else:
            # Si es un solo registro, lo manejamos igual
            future = publisher.publish(topic_path, payload)
            publish_futures.append(future)

        # --- ESPERA CONJUNTA ---
        # Después de haber disparado todas las publicaciones, ahora sí esperamos
        # a que todas terminen.
        for future in publish_futures:
            future.result() # El .result() aquí bloqueará hasta que este futuro específico esté completo.

        success_message = f"{len(publish_futures)} mensajes publicados exitosamente en Pub/Sub."
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