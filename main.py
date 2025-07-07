from google.cloud import pubsub_v1
import json
import os

# --- INICIALIZACIÓN MEJORADA ---
# Obtener el ID del proyecto desde la variable de entorno que GCP garantiza que existe.
project_id = os.environ.get('GCP_PROJECT') 
# El topic_id lo podemos hardcodear porque es parte de nuestra arquitectura.
topic_id = "registros-produccion"

# Crear el cliente y el topic_path una sola vez.
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def main(request):
    """
    Función que recibe un lote de registros y publica cada uno individualmente
    utilizando una lógica de publicación síncrona y simple.
    """
    # Primero, verificar si el payload está vacío (para el handshake del webhook)
    if not request.content_length:
        print("Petición de verificación recibida, respondiendo OK.")
        return "Endpoint verificado", 200

    try:
        data = request.get_json(silent=True)
        if data is None:
            # Si get_json falla (ej. no es un JSON válido), data es None.
            print(f"Error: Payload no es un JSON válido. Payload: {request.get_data(as_text=True)}")
            return "Error: Payload no es un JSON válido.", 400

        # --- LÓGICA DE PUBLICACIÓN SIMPLE (como la de la profesora) ---
        if isinstance(data, list):
            print(f"Lote de {len(data)} registros recibido. Publicando uno por uno...")
            for item in data:
                message_bytes = json.dumps(item).encode("utf-8")
                future = publisher.publish(topic_path, message_bytes)
                future.result()  # Esperar a que esta publicación específica se complete.
        else: # Si es un solo objeto JSON
            print("Registro único recibido. Publicando...")
            message_bytes = json.dumps(data).encode("utf-8")
            future = publisher.publish(topic_path, message_bytes)
            future.result()

        print("Proceso completado exitosamente.")
        return "Completado", 200

    except Exception as e:
        # Captura cualquier otro error inesperado.
        print(f"Error inesperado al procesar la solicitud: {e}")
        return f"Error al procesar la solicitud: {e}", 500