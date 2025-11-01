import math


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Esta función calcula la distancia en km entre dos puntos geográficos usando la fórmula del haversine
    # Usaremos esta función para ordenar las propiedades por distancia
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))
