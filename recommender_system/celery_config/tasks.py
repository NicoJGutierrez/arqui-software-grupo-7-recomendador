# celery
from recommender_system.celery_app import app

# standard
import logging
from typing import List, Dict, Any

from recommender_system.celery_config.controllers import haversine

logger = logging.getLogger(__name__)


@app.task(bind=False)
def compute_recommendations(user_id: int, property_id: int, job_id: int = None, all_properties: List[Dict[str, Any]] = None):
    """
    Tarea Celery que calcula hasta 3 recomendaciones usando la lista `all_properties` proporcionada.
    Mantiene la aproximación original (sin usar sessions/DB dentro del worker).

    Reglas:
      - misma comuna
      - mismo número de `bedrooms`
      - `price` <= precio de la propiedad origen
      - ordenar por distancia asc (si hay coordenadas), luego por precio asc

    Devuelve una lista con hasta 3 elementos: cada uno es un dict {"property": <prop>, "distance_km": <km>}.
    """
    if not all_properties:
        return "error: no properties provided"

    # buscar propiedad origen en la lista
    origen = next(
        (p for p in all_properties if p.get("external_id") == property_id), None)
    if not origen:
        return "error: property not found"

    # filtrar candidatos
    candidates = [p for p in all_properties if
                  p.get("comuna") == origen.get("comuna") and
                  p.get("bedrooms") == origen.get("bedrooms") and
                  (p.get("price") is not None and origen.get("price") is not None and p.get("price") <= origen.get("price")) and
                  p.get("external_id") != origen.get("external_id")]

    scored = []
    for p in candidates:
        # usar 'lat'/'lon'
        lat1 = origen.get("lat")
        lon1 = origen.get("lon")
        lat2 = p.get("lat")
        lon2 = p.get("lon")
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            # si faltan coordenadas, omitimos para priorizar candidatos con coordenadas
            continue

        try:
            dist = haversine(float(lat1), float(
                lon1), float(lat2), float(lon2))
        except Exception:
            continue
        scored.append((p, dist))

    # ordenar por distancia y luego precio
    scored.sort(key=lambda x: (x[1], x[0].get("price") if x[0].get(
        "price") is not None else float('inf')))

    top3 = scored[:3]

    # serializar resultado
    result = []
    for prop, dist in top3:
        result.append({
            "property": prop,
            "distance_km": dist
        })

    return result
