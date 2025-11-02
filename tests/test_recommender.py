import pytest
from fastapi.testclient import TestClient
from recommender_system.recommender_master import app
from recommender_system.database import SessionLocal, init_db
from recommender_system.models import Property
from recommender_system.celery_config.tasks import compute_recommendations
import time
import os

# Configurar variables de entorno para pruebas locales ANTES de importar
os.environ["DATABASE_URL"] = "sqlite:///./test.db"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["CELERY_RESULT_BACKEND"] = "cache+memory://"

# Crear un cliente de prueba para FastAPI
client = TestClient(app)

# Configurar la base de datos para pruebas


@pytest.fixture(scope="module", autouse=True)
def setup_database():
    # Inicializar la base de datos
    init_db()
    yield
    # Limpiar la base de datos después de todos los tests
    with SessionLocal() as session:
        session.query(Property).delete()
        session.commit()


def test_post_property():
    """Probar el endpoint POST /recommender/properties/notify"""
    payload = {
        "external_id": 2001,
        "comuna": "Norte",
        "lat": -33.45,
        "lon": -70.65,
        "bedrooms": 2,
        "price": 95000.0,
        "raw": {"description": "Propiedad de prueba"}
    }

    response = client.post("/recommender/properties/notify", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ["created", "updated"]
    assert "id" in data


def test_get_properties():
    """Probar el endpoint GET /recommender/properties"""
    response = client.get("/recommender/properties")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert "external_id" in data[0]
    assert "comuna" in data[0]
    assert "lat" in data[0]
    assert "lon" in data[0]
    assert "bedrooms" in data[0]
    assert "price" in data[0]


def test_recommendation_worker():
    """Probar que se puede crear un job de recomendación."""
    # Crear un job de recomendación
    user_id = 1
    property_id = 2001  # Debe coincidir con una propiedad existente
    response = client.post(f"/recommender/job/{user_id}/{property_id}")
    assert response.status_code == 200
    data = response.json()
    assert "task_id" in data
    assert "status" in data

    # Verificar que el job se creó
    task_id = data["task_id"]
    response = client.get(f"/recommender/job/{task_id}")
    assert response.status_code == 200
    job_data = response.json()
    assert "ready" in job_data
    assert "status" in job_data
    assert job_data["status"] in ["PENDING", "STARTED", "SUCCESS", "FAILURE"]


def test_recommendation_result():
    """Probar que se puede crear un job de recomendación."""
    # Limpiar la base de datos antes de este test
    with SessionLocal() as session:
        session.query(Property).delete()
        session.commit()

    # Insertar propiedades de prueba con variedad en comunas, precios y localizaciones
    properties = [
        {
            "external_id": 3001,
            "comuna": "Centro",
            "lat": -33.45,
            "lon": -70.65,
            "bedrooms": 2,
            "price": 100000.0,
            "raw": {"description": "Propiedad base en Centro"}
        },
        {
            "external_id": 3002,
            "comuna": "Centro",
            "lat": -33.46,
            "lon": -70.66,
            "bedrooms": 2,
            "price": 95000.0,
            "raw": {"description": "Propiedad similar en Centro"}
        },
        {
            "external_id": 3003,
            "comuna": "Norte",
            "lat": -33.35,
            "lon": -70.55,
            "bedrooms": 3,
            "price": 150000.0,
            "raw": {"description": "Propiedad en Norte con precio alto"}
        },
        {
            "external_id": 3004,
            "comuna": "Sur",
            "lat": -33.55,
            "lon": -70.75,
            "bedrooms": 1,
            "price": 60000.0,
            "raw": {"description": "Propiedad en Sur con precio bajo"}
        },
        {
            "external_id": 3005,
            "comuna": "Providencia",
            "lat": -33.42,
            "lon": -70.62,
            "bedrooms": 4,
            "price": 200000.0,
            "raw": {"description": "Propiedad en Providencia con precio muy alto"}
        },
        {
            "external_id": 3006,
            "comuna": "Las Condes",
            "lat": -33.38,
            "lon": -70.52,
            "bedrooms": 2,
            "price": 120000.0,
            "raw": {"description": "Propiedad en Las Condes"}
        }
    ]

    for prop in properties:
        client.post("/recommender/properties/notify", json=prop)

    # Verificar que las propiedades se insertaron
    response = client.get("/recommender/properties")
    assert response.status_code == 200
    props_in_db = response.json()
    assert len(props_in_db) == 6
    assert any(p["external_id"] == 3001 for p in props_in_db)

    # Crear un job de recomendación para una propiedad específica
    user_id = 1
    property_id = 3001  # Propiedad base para la recomendación

    # Obtener la propiedad origen para verificar comuna
    origen = next(p for p in props_in_db if p["external_id"] == property_id)

    # Llamar la función de recomendación directamente para pruebas
    result = compute_recommendations(
        user_id, property_id, all_properties=props_in_db)

    # Verificar el resultado
    assert isinstance(result, list)
    assert len(result) <= 3  # El máximo de recomendaciones es 3
    for rec in result:
        assert "property" in rec
        assert "distance_km" in rec
        assert "knn_distance" in rec
        prop = rec["property"]
        assert isinstance(rec["distance_km"], (int, float))
        assert rec["distance_km"] >= 0
        assert isinstance(rec["knn_distance"], (int, float))
        assert rec["knn_distance"] >= 0
        # Verificar que la propiedad recomendada no es la origen
        assert prop["external_id"] != property_id
        # Verificar que la propiedad está en la misma comuna que la origen
        assert prop["comuna"] == origen["comuna"]
        # Verificar que la propiedad tiene los campos necesarios
        assert "external_id" in prop
        assert "comuna" in prop
        assert "lat" in prop
        assert "lon" in prop
        assert "bedrooms" in prop
        assert "price" in prop

    # Imprimir las propiedades recomendadas con sus datos y puntuación KNN
    print("\nPropiedades recomendadas:")
    for i, rec in enumerate(result, 1):
        prop = rec["property"]
        print(f"{i}. ID: {prop['external_id']}, Comuna: {prop['comuna']}, Lat: {prop['lat']}, Lon: {prop['lon']}, Bedrooms: {prop['bedrooms']}, Price: {prop['price']}, Distance KM: {rec['distance_km']:.2f}, KNN Distance: {rec['knn_distance']:.4f}")
