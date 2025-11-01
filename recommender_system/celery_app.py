import os
from celery import Celery

# Crear la instancia de Celery usando variables de entorno (o valores por defecto)
BROKER_URL = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', BROKER_URL)

app = Celery('recommender_system', broker=BROKER_URL, backend=RESULT_BACKEND)

# configuración mínima coherente con el proyecto
app.conf.update(
    accept_content=['json'],
    task_serializer='json',
    result_serializer='json',
    timezone='America/Santiago',
)

# Autodiscover tasks en el paquete de configuración
try:
    app.autodiscover_tasks(['recommender_system.celery_config'])
except Exception:
    # fallback silencioso si el entorno de importación es diferente en pruebas locales
    pass

# Exponer `app` como la variable que el resto del proyecto importa
# Ej: from recommender_system.celery_app import app
