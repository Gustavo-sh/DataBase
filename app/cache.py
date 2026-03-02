from collections import OrderedDict
from datetime import datetime, timedelta
import fakeredis.aioredis as fakeredis
from fastapi import Request
import json
from datetime import timedelta
import redis.asyncio as redis
from datetime import datetime, date
import os

CACHE = OrderedDict()
CACHE_TTL = timedelta(minutes=5)
CACHE_MAX_SIZE = 100
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# redis_client = fakeredis.FakeRedis()

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

async def set_cache(key: str, valor, ttl: int = 60):
    await redis_client.set(
    key,
    json.dumps(valor, default=json_serial),
    ex=ttl
)

async def get_from_cache(key: str):
    data = await redis_client.get(key)
    return json.loads(data) if data else None

async def load_registros(request: Request):
    username = request.cookies.get("username", "anon")
    data = await redis_client.get(f"registros:{username}")
    return json.loads(data) if data else []

async def save_registros(request: Request, registros):
    ttl = 900
    username = request.cookies.get("username", "anon")
    await redis_client.set(f"registros:{username}", json.dumps(registros, default=json_serial), ex=ttl)

async def get_current_user(request: Request):

    session_token = request.cookies.get("session_token")

    if not session_token:
        return None

    session_data = await redis_client.get(f"session:{session_token}")

    if not session_data:
        return None

    return json.loads(session_data)

def json_serial(obj):
    """
    Função de serialização JSON para objetos que o json padrão não suporta.
    Converte objetos datetime e date para o formato de string ISO 8601.
    """
    if isinstance(obj, (datetime, date)):
        # Converte para string no formato ISO 8601 (ex: '2025-10-16T12:00:00')
        return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')