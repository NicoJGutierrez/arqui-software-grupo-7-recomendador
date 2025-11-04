# Este código recomienda propiedades similares basadas en la última propiedad comprada por el usuario. Se debe ejecutar después de que se agende una propiedad para un usuario determinado.

# Este script recibe un id de usuario y propiedad, y usando los parámetros de la propiedad, recomienda otras propiedades similares siguiendo el algoritmo a continuación:
# 1. Obtener la comuna, ubicación geográfica, número de dormitorios y precio de la propiedad que se acaba de agendar.
# 2. Usar KNN para encontrar las 3 propiedades más similares basadas en latitud, longitud y precio.
# 3. Calcular distancia geográfica real para cada recomendación.

# Esto se debe implementar mediante workers con celery, de modo que cuando se agende una propiedad para un usuario, se dispare un worker que realice esta recomendación y la almacene en una tabla de recomendaciones.

# Se deben ofrecer además los siguientes endpoints desde el maestro de los workers:
# GET /job/(:id) Donde :id representa el id de un job creado
# POST /job Recibe los datos necesarios para el pago y entrega un id del job creado
# GET /heartbeat Indica si el servicio está operativo (devuelve true)

import logging  # Agrega esta importación si no está
from recommender_system.celery_app import app as celery_app
from recommender_system.celery_config.tasks import compute_recommendations
import json
from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, FastAPI
import sys
import os
from typing import Optional, List, Union
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
import re
import traceback  # Agrega esta importación

# DB imports
from recommender_system.database import SessionLocal, init_db
from recommender_system.models import Property

# Asegurar que el paquete `API` (y su subpaquete `database`) esté en sys.path
# Esto permite importaciones como `from database.connection import SessionLocal`
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
api_path = os.path.join(base_dir, "API")
if api_path not in sys.path:
    sys.path.insert(0, api_path)

# importa la tarea Celery (la tarea ahora está registrada en el celery app)

router = APIRouter(prefix="/recommender", tags=["recommender"])

# Inicializar la DB (crea tablas si no existen)
try:
    init_db()
except Exception:
    # no detener la importación si la db no está accesible en este momento
    pass


# Configura el logger al inicio del archivo
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@router.post("/job/{user_id}/{property_id}")
def create_job(user_id: str, property_id: int):
    """
    Encola la tarea de recomendación. Devuelve el task_id de Celery para seguimiento.
    """
    logger.info(
        f"Iniciando creación de job para user_id={user_id}, property_id={property_id}")
    # Obtener todas las propiedades desde la base de datos local
    all_props = []
    try:
        with SessionLocal() as session:
            props = session.query(Property).all()
            all_props = [p.to_dict() for p in props]
            logger.info(f"Obtenidas {len(all_props)} propiedades de la DB")
    except Exception as e:
        logger.error(f"Error al obtener propiedades de DB: {str(e)}")
        all_props = []
        print("Warning: no se pudo leer la base de datos local de propiedades.")

    # Encolar la tarea pasando las propiedades obtenidas
    try:
        async_result = compute_recommendations.apply_async(
            args=[user_id, property_id], kwargs={"all_properties": all_props}
        )
        logger.info(
            f"Job encolado exitosamente, task_id={async_result.id}, status={async_result.status}")
        return {"task_id": async_result.id, "status": async_result.status}
    except Exception as e:
        logger.error(f"Error al encolar job: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error encolando job: {str(e)}")


class PropertyNotify(BaseModel):
    """Modelo sencillo para recibir notificaciones de propiedades."""
    external_id: Optional[int]
    comuna: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    bedrooms: Optional[Union[int, str]]  # Permite int o str
    price: Optional[float]
    # Agrega default para evitar error de campo requerido
    raw: Optional[dict] = None


def parse_bedrooms(bedrooms_value):
    """Parsea bedrooms: extrae int de string (e.g., '1 dormitorio' -> 1), o devuelve int/None."""
    if isinstance(bedrooms_value, str):
        match = re.match(r'^(\d+)', bedrooms_value)  # Extrae dígitos al inicio
        return int(match.group(1)) if match else None
    elif isinstance(bedrooms_value, int):
        return bedrooms_value
    return None


@router.post("/properties/notify")
def notify_property(payload: PropertyNotify):
    """Endpoint para insertar/actualizar una propiedad cuando otra API notifica un cambio.

    - Si `external_id` coincide con una propiedad existente, se actualiza.
    - Si no existe, se crea un nuevo registro.
    """
    if payload.external_id is None:
        raise HTTPException(status_code=400, detail="external_id is required")

    try:
        with SessionLocal() as session:
            print("hay session")
            prop = session.query(Property).filter(
                Property.external_id == payload.external_id).one_or_none()
            if prop is None:
                print("creando propiedad")
                parsed_bedrooms = parse_bedrooms(payload.bedrooms)
                prop = Property(
                    external_id=payload.external_id,
                    comuna=payload.comuna,
                    lat=payload.lat,
                    lon=payload.lon,
                    bedrooms=parsed_bedrooms,
                    price=payload.price,
                )
                session.add(prop)
                session.commit()
                session.refresh(prop)
                return {"status": "created", "id": prop.id}
            else:
                print("actualizando propiedad")
                # actualizar campos si vienen en el payload
                if payload.comuna is not None:
                    setattr(prop, "comuna", payload.comuna)
                if payload.lat is not None:
                    setattr(prop, "lat", payload.lat)
                if payload.lon is not None:
                    setattr(prop, "lon", payload.lon)
                parsed_bedrooms = parse_bedrooms(payload.bedrooms)
                if parsed_bedrooms is not None:
                    setattr(prop, "bedrooms", parsed_bedrooms)
                if payload.price is not None:
                    setattr(prop, "price", payload.price)
                if payload.raw is not None:
                    setattr(prop, "raw", payload.raw)
                session.add(prop)
                session.commit()
                return {"status": "updated", "id": prop.id}
    except Exception as e:
        import logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        logger.error(f"Error en notify_property: {str(e)}")
        # Agrega traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/job/{task_id}")
def get_job(task_id: str):
    """Consulta el estado del task de Celery usando su id."""
    logger.info(f"Consultando estado de job con task_id={task_id}")
    try:
        result = celery_app.AsyncResult(task_id)
        logger.info(
            f"Estado del job {task_id}: resultado={result.result}, status={result.status}")
        return {"ready": result.ready(), "status": result.status, "result": result.result}
    except Exception as e:
        logger.error(f"Error al consultar job {task_id}: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Error consultando job: {str(e)}")


@router.get("/heartbeat")
def heartbeat():
    return {"alive": True}


@router.get("/properties")
def get_all_properties():
    """Devuelve todas las propiedades almacenadas en la base de datos."""
    try:
        with SessionLocal() as session:
            properties = session.query(Property).all()
            return [prop.to_dict() for prop in properties]
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error al obtener propiedades: {str(e)}")


# Exponer la aplicación FastAPI para poder ejecutar este servicio por separado
app = FastAPI(title="recommender-master")


@app.middleware("http")
async def log_requests(request, call_next):
    logger.info(
        f"Request: {request.method} {request.url} - Headers: {dict(request.headers)}")
    response = await call_next(request)
    logger.info(
        f"Response: {response.status_code} for {request.method} {request.url}")
    return response

app.include_router(router)
