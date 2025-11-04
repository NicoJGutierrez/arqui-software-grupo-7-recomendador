# celery
from recommender_system.celery_app import app

# standard
import logging
from typing import List, Dict, Any, Optional
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

from recommender_system.celery_config.controllers import haversine

logger = logging.getLogger(__name__)


@app.task(bind=False)
def compute_recommendations(user_id: int, property_id: int, job_id: Optional[int] = None, all_properties: Optional[List[Dict[str, Any]]] = None):
    """
    Tarea Celery que calcula hasta 3 recomendaciones usando KNN con la lista `all_properties` proporcionada.

    Reglas:
      - Filtrar propiedades en la misma comuna que la propiedad origen
      - Usar KNN para encontrar las 3 más similares basadas en lat, lon, price

    Devuelve una lista con hasta 3 elementos: cada uno es un dict {"property": <prop>, "distance_km": <km>, "knn_distance": <dist>}.
    """
    if not all_properties:
        return "error: no properties provided"

    # buscar propiedad origen en la lista
    origen = next(
        (p for p in all_properties if p.get("external_id") == property_id), None)
    if not origen:
        return "error: property not found"

    origen_price = origen.get("price")
    origen_comuna = origen.get("comuna")
    candidates = []
    for p in all_properties:
        if (p.get("external_id") != origen.get("external_id")):
            candidates.append(p)

    if not candidates:
        return []

    # preparar datos para KNN
    # características: lat, lon, price
    features = []
    for p in candidates:
        lat = p.get("lat") or 0
        lon = p.get("lon") or 0
        price = p.get("price") or 0
        features.append([lat, lon, price])

    # incluir la propiedad origen para calcular distancias
    origen_lat = origen.get("lat") or 0
    origen_lon = origen.get("lon") or 0
    origen_price = origen.get("price") or 0
    origen_features = [[origen_lat, origen_lon, origen_price]]
    all_features = origen_features + features

    # convertir a numpy array
    all_features = np.array(all_features)

    # normalizar características
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(all_features)

    # usar KNN para encontrar los 3 más cercanos a la propiedad origen
    knn = NearestNeighbors(n_neighbors=min(
        4, len(candidates) + 1), algorithm='auto')  # +1 porque incluye origen
    knn.fit(scaled_features)
    distances, indices = knn.kneighbors(
        scaled_features[0:1])  # solo para origen

    # obtener los candidatos más cercanos (excluyendo el origen mismo)
    recommended_indices = indices[0][1:]  # saltar el primero que es el origen
    recommended_distances = distances[0][1:]

    # calcular distancia geográfica real para cada recomendación
    result = []
    for idx, dist in zip(recommended_indices, recommended_distances):
        # porque indices empiezan desde 1 (origen es 0)
        prop = candidates[idx - 1]
        # calcular distancia real usando haversine
        real_dist = haversine(
            origen_lat, origen_lon, prop["lat"], prop["lon"])
        result.append({
            "property": prop,
            "distance_km": real_dist,
            "knn_distance": dist
        })

    print(
        f"Recomendaciones para user_id={user_id}, property_id={property_id}: {result}")
    return result
