from collections import OrderedDict
from datetime import datetime, timedelta
import fakeredis
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

#redis_client = fakeredis.FakeRedis()

# redis_client = redis.StrictRedis(
#     host='localhost',
#     port=6379, # Porta padrão, mas pode mudar
#     db=0,
#     decode_responses=True
# )

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

# CACHE_TTL_24H = timedelta(hours=24).total_seconds() # 86400 segundos
SESSION_PREFIX = "session:"

# def set_cache_24h(key: str, valor):
#     """Salva um valor no Redis com expiração de 24 horas."""
#     # O Redis armazena strings, então convertemos para JSON
#     data = json.dumps(valor)
#     redis_client.set(key, data, ex=int(CACHE_TTL_24H)) # 'ex' define o tempo de expiração em segundos

# def get_from_cache(key: str):
#     if key in CACHE:
#         valor, expira_em = CACHE[key]
#         if datetime.utcnow() < expira_em:
#             CACHE.move_to_end(key)
#             return valor
#         del CACHE[key]
#     return None

# def set_cache(key: str, valor, ttl: timedelta = CACHE_TTL):
#     if key in CACHE:
#         del CACHE[key]
#     elif len(CACHE) >= CACHE_MAX_SIZE:
#         CACHE.popitem(last=False)
#     CACHE[key] = (valor, datetime.utcnow() + ttl)

async def set_cache(key: str, valor, ttl: int = 60):
    await redis_client.set(
    key,
    json.dumps(valor, default=json_serial),
    ex=ttl
)

async def get_from_cache(key: str):
    data = await redis_client.get(key)
    return json.loads(data) if data else None

def get_user_key(request: Request):
    username = request.cookies.get("username", "anon")
    return f"registros:{username}"

async def load_registros(request: Request):
    key = get_user_key(request)
    data = await redis_client.get(key)
    return json.loads(data) if data else []

async def save_registros(request: Request, registros):
    ttl = 900
    key = get_user_key(request)
    await redis_client.set(key, json.dumps(registros, default=json_serial), ex=ttl)

async def set_session(token: str, data: dict):
    await redis_client.set(f"{SESSION_PREFIX}{token}", json.dumps(data))

async def get_session(token: str):
    data = await redis_client.get(f"{SESSION_PREFIX}{token}")
    return json.loads(data) if data else None

async def get_current_user(request: Request):
    token = request.cookies.get("session_token")
    if not token:
        return None
    return await get_session(token)

def json_serial(obj):
    """
    Função de serialização JSON para objetos que o json padrão não suporta.
    Converte objetos datetime e date para o formato de string ISO 8601.
    """
    if isinstance(obj, (datetime, date)):
        # Converte para string no formato ISO 8601 (ex: '2025-10-16T12:00:00')
        return obj.isoformat()
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')