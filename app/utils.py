from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros, set_session, get_current_user
)

def _check_role_or_forbid(user: dict, allowed_roles: list[str]):
    """
    Lança HTTPException(403) se o usuário não estiver autenticado ou não tiver a role permitida.
    """
    if not user:
        return False
    if user.get("role") not in allowed_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Acesso negado.")
    return True

async def _check_registro_scope(request, registro_id, user):
    registros = await load_registros(request)
    if not any(str(r["id"]) == registro_id for r in registros):
        raise HTTPException(status_code=403, detail="Acesso negado")
    

def parse_date_safe(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str) and value:
        return datetime.strptime(value, "%Y-%m-%d")
    return None

def preprocess_registros(registros):
    por_atributo = defaultdict(list)
    por_indicador = defaultdict(list)

    for r in registros:
        por_atributo[r["atributo"]].append(r)
        por_indicador[(r["atributo"], r["id_nome_indicador"])].append(r)

        r["_dt_inicio"] = parse_date_safe(r.get("data_inicio"))
        r["_dt_fim"] = parse_date_safe(r.get("data_fim"))

    return por_atributo, por_indicador

def require_htmx(request: Request):
    if request.headers.get("HX-Request") != "true":
        raise HTTPException(status_code=403, detail="Requisição inválida")
    
def validate_origin(request: Request):
    origin = request.headers.get("origin") or request.headers.get("referer")
    if not origin:
        raise HTTPException(status_code=403)

    # if not origin.startswith("https://seusistema.interno"):
    #     raise HTTPException(status_code=403)

def clean_value(v):
        if isinstance(v, str):
            v = v.strip().replace("–", "-").replace("—", "-")
            v = v.replace("\n", " ").replace("\r", " ").replace("\xa0", " ")
            if v.lower() in ("nan", "none", "null", ""):
                return ""
            return v
        if pd.isna(v):
            return ""
        return v

def to_int_safe(v):
        try:
            if v == "" or pd.isna(v):
                return 0
            return int(float(v))
        except Exception:
            return 0