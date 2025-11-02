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

from recommender_system.celery_app import app as celery_app
from recommender_system.celery_config.tasks import compute_recommendations
import json
from datetime import datetime
from uuid import uuid4
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, FastAPI
import sys
import os
from typing import Optional, List

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


@router.post("/job/{user_id}/{property_id}")
def create_job(user_id: int, property_id: int):
    """
    Encola la tarea de recomendación. Devuelve el task_id de Celery para seguimiento.
    """
    # Obtener todas las propiedades desde la base de datos local
    all_props = []
    try:
        with SessionLocal() as session:
            props = session.query(Property).all()
            all_props = [p.to_dict() for p in props]
    except Exception:
        # si falla la lectura de BD, dejamos all_props = [] y el worker podrá fallar/registrar
        all_props = []

    # Encolar la tarea pasando las propiedades obtenidas
    async_result = compute_recommendations.apply_async(
        args=[user_id, property_id], kwargs={"all_properties": all_props}
    )
    return {"task_id": async_result.id, "status": async_result.status}


class PropertyNotify(BaseModel):
    """Modelo sencillo para recibir notificaciones de propiedades."""
    external_id: Optional[int]
    comuna: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    bedrooms: Optional[int]
    price: Optional[float]
    raw: Optional[dict]


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
            prop = session.query(Property).filter(
                Property.external_id == payload.external_id).one_or_none()
            if prop is None:
                prop = Property(
                    external_id=payload.external_id,
                    comuna=payload.comuna,
                    lat=payload.lat,
                    lon=payload.lon,
                    bedrooms=payload.bedrooms,
                    price=payload.price,
                    raw=payload.raw,
                )
                session.add(prop)
                session.commit()
                session.refresh(prop)
                return {"status": "created", "id": prop.id}
            else:
                # actualizar campos si vienen en el payload
                if payload.comuna is not None:
                    prop.comuna = payload.comuna
                if payload.lat is not None:
                    prop.lat = payload.lat
                if payload.lon is not None:
                    prop.lon = payload.lon
                if payload.bedrooms is not None:
                    prop.bedrooms = payload.bedrooms
                if payload.price is not None:
                    prop.price = payload.price
                if payload.raw is not None:
                    prop.raw = payload.raw
                session.add(prop)
                session.commit()
                return {"status": "updated", "id": prop.id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/job/{task_id}")
def get_job(task_id: str):
    """Consulta el estado del task de Celery usando su id."""
    result = celery_app.AsyncResult(task_id)
    return {"ready": result.ready(), "status": result.status, "result": result.result}


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
app.include_router(router)
