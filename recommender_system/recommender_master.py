# Este código recomienda propiedades similares basadas en la última propiedad comprada por el usuario. Se debe ejecutar después de que se agende una propiedad para un usuario determinado.

# Este script recibe un id de usuario y propiedad, y usando los parámetros de la propiedad, recomienda otras propiedades similares siguiendo el algoritmo a continuación:
# 1. Obtener la comuna, ubicación geográfica, número de dormitorios y precio de la propiedad que se acaba de agendar.
# 2. Deben filtrar, entre todas las propiedades de su sistema, por aquellas con el mismo número de dormitorios, que su precio no sea mayor al de la propiedad del paso 1, y que su parámetro comuna sea igual al de la propiedad recientemente agendada.
# 3. Deben ordenar las propiedades según que tan cercanas son geográficamente a la propiedad recientemente agendada y según su precio de menor a mayor.
# 4. Deben obtener las 3 primeras coincidencias, si no hay coincidencias se debe indicar.

# Esto se debe implementar mediante workers con celery, de modo que cuando se agende una propiedad para un usuario, se dispare un worker que realice esta recomendación y la almacene en una tabla de recomendaciones.

# Se deben ofrecer además los siguientes endpoints desde el maestro de los workers:
# GET /job/(:id) Donde :id representa el id de un job creado
# POST /job Recibe los datos necesarios para el pago y entrega un id del job creado
# GET /heartbeat Indica si el servicio está operativo (devuelve true)

from recommender_system.celery_app import app as celery_app
from recommender_system.celery_config.tasks import compute_recommendations
import requests
import json
from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, FastAPI
import sys
import os

# Asegurar que el paquete `API` (y su subpaquete `database`) esté en sys.path
# Esto permite importaciones como `from database.connection import SessionLocal`
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
api_path = os.path.join(base_dir, "API")
if api_path not in sys.path:
    sys.path.insert(0, api_path)

# importa la tarea Celery (la tarea ahora está registrada en el celery app)

router = APIRouter(prefix="/recommender", tags=["recommender"])


@router.post("/job/{user_id}/{property_id}")
def create_job(user_id: int, property_id: int):
    """
    Encola la tarea de recomendación. Devuelve el task_id de Celery para seguimiento.
    """
    # intentar obtener la lista de propiedades desde la API para que el worker reciba `all_properties` y pueda calcular las recomendaciones correctamente
    api_url = os.getenv("API_URL", "http://properties_api:8000")
    all_props = None
    try:
        # el API limita `limit` a 100 por request, respetamos eso
        resp = requests.get(f"{api_url}/properties?limit=100", timeout=10)
        if resp.ok:
            body = resp.json()
            # la ruta devuelve {"total": X, "results": [...]}
            all_props = body.get("results") if isinstance(body, dict) else None
    except Exception:
        # no rompemos la creación del job si la llamada falla; el worker podrá retornar un error describiendo que no recibió propiedades
        all_props = None

    # usar apply_async con argumentos en lista y pasar all_properties como kwarg
    async_result = compute_recommendations.apply_async(
        args=[user_id, property_id], kwargs={"all_properties": all_props}
    )
    return {"task_id": async_result.id, "status": async_result.status}


@router.get("/job/{task_id}")
def get_job(task_id: str):
    """Consulta el estado del task de Celery usando su id."""
    result = celery_app.AsyncResult(task_id)
    return {"ready": result.ready(), "status": result.status, "result": result.result}


@router.get("/heartbeat")
def heartbeat():
    return {"alive": True}


# Exponer la aplicación FastAPI para poder ejecutar este servicio por separado
app = FastAPI(title="recommender-master")
app.include_router(router)
