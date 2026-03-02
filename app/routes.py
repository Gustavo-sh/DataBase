import copy
from fastapi import APIRouter, Request, Form, Query, HTTPException, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime, timedelta
from passlib.context import CryptContext
from urllib.parse import urlparse
import uuid
from typing import List 
import pandas as pd
from io import BytesIO
from fastapi.responses import StreamingResponse
import json
from fastapi import UploadFile, File
from html import escape
from app.cache import (
    get_from_cache, set_cache, load_registros, save_registros, get_current_user, redis_client
)
from app.connections_db import (
    get_indicadores, get_funcao, get_atributos_matricula, get_user_bd, save_user_bd, save_registros_bd, get_operacoes, get_gerentes,
    get_atributos_adm, update_da_adm_apoio, get_num_atendentes, import_from_excel,
    get_atributos_apoio, get_atributos_gerente, update_meta_moedas_bd, get_names, get_all_alterations,
    get_all_atributos_cadastro_apoio, get_matrizes_nao_cadastradas, get_matrizes_alteradas_apoio, update_dmm_bd, query_mes, 
    get_factibilidade, insert_log_auditoria,check_atribute_and_periodo_bd,get_pendencias_apoio,
)
from app.validations import validation_submit_table, validation_import_from_excel, validation_meta_moedas, validation_dmm
from app.utils import _check_registro_scope, _check_role_or_forbid, preprocess_registros, require_htmx, validate_origin, clean_value, to_int_safe, generate_cache_key

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
adms = ["277561", "117699", "154658", "160031", "086939", "429569"]

CACHE_TTL = timedelta(minutes=5)
SESSION_TIMEOUT = 900
EXPECTED_COLUMNS = [
    'atributo', 'id_nome_indicador', 'meta_sugerida', 'resultado', 'atingimento', 'meta', 'moedas', 'tipo_indicador', 
        'acumulado', 'esquema_acumulado', 'tipo_matriz', 'data_inicio', 
        'data_fim', 'periodo', 'escala', 'tipo_de_faturamento', 
        'descricao', 'ativo', 'chamado', 'criterio', 'gerente', 'possui_dmm', 'dmm', 'submetido_por', 
        'data_submetido_por', 'qualidade', 'da_qualidade', 'data_da_qualidade', 
        'planejamento', 'da_planejamento', 'data_da_planejamento', 'exop', 'da_exop', 'data_da_exop'
]
EXPECTED_COLUMNS_IMPORT = [
    'atributo', 'id_nome_indicador', 'meta', 'moedas', 'tipo_indicador', 
        'acumulado', 'esquema_acumulado', 'tipo_matriz', 'data_inicio', 
        'data_fim', 'periodo', 'escala', 'tipo_de_faturamento', 
        'descricao', 'ativo', 'chamado', 'criterio', 'gerente', 'possui_dmm', 'dmm', 'submetido_por', 
        'data_submetido_por', 'qualidade', 'da_qualidade', 'data_da_qualidade', 
        'planejamento', 'da_planejamento', 'data_da_planejamento', 'exop', 'da_exop', 'data_da_exop'
]

@router.post("/delete/{id}", response_class=HTMLResponse)
async def delete_registro(request: Request, id: str):
    registros = await load_registros(request)
    registros = [r for r in registros if str(r["id"]) != str(id)]
    await save_registros(request, registros)
    return templates.TemplateResponse("_registro.html", {"request": request, "registros": registros})

@router.get("/")
def home():
    return RedirectResponse("/login", status_code=303)

@router.get("/login")
def login_page(request: Request, msg: Optional[str] = Query(None), erro: Optional[str] = Query(None)):
    return templates.TemplateResponse("login.html", {"request": request, "msg": msg, "erro": erro})

@router.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    user = await get_user_bd(username)

    if not user:
        return RedirectResponse("/login?erro=Usuário não cadastrado!", status_code=303)

    if not pwd_context.verify(password, user["password"]):
        return RedirectResponse("/login?erro=Senha incorreta!", status_code=303)

    session_token = str(uuid.uuid4())

    await redis_client.set(
        f"session:{session_token}",
        json.dumps({
            "usuario": username,
            "role": user.get("role")
        }),
        ex=SESSION_TIMEOUT
    )

    resp = RedirectResponse("/redirect_by_role", status_code=303)

    resp.set_cookie(
        "session_token",
        session_token,
        httponly=True,
        #secure=True,
        samesite="Lax",
    )
    resp.set_cookie(
        "username",
        username,
        httponly=True,
        #secure=True,
        samesite="Lax",
    )

    return resp

@router.get("/redirect_by_role")
async def redirect_by_role(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    role = user.get("role")
    if role == "operacao":
        return RedirectResponse("/matriz/operacao")
    elif role in ["apoio qualidade", "apoio planejamento"]:
        return RedirectResponse("/matriz/apoio")
    elif role == "adm":
        return RedirectResponse("/matriz/adm/acordo")
    else:
        raise HTTPException(status_code=403, detail="Role inválida")

@router.post("/logout")
async def logout(request: Request):

    session_token = request.cookies.get("session_token")

    if session_token:
        await redis_client.delete(f"session:{session_token}")

    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session_token")
    await save_registros(request, [])

    return response

@router.get("/register")
def register_page(request: Request, erro: Optional[str] = Query(None)):
    return templates.TemplateResponse("register.html", {"request": request, "erro": erro})

@router.post("/register")
async def register_user(request: Request, username: str = Form(...), password: str = Form(...)):
    if await get_user_bd(username):
        return RedirectResponse("/register?erro=Usuário já cadastrado!", status_code=303)
    role = None
    if username in adms:
        role = "adm"
    else:
        funcao = None
        try:
            funcao = await get_funcao(username)
        except Exception as e:
            funcao = f"Erro ao obter funcao: {e}"
        funcao_upper = funcao.upper() if funcao else ""
        if "COORDENADOR DE QUALIDADE" in funcao_upper or "GERENTE DE QUALIDADE" in funcao_upper:
            role = "apoio qualidade"
        elif "COORDENADOR DE PLANEJAMENTO" in funcao_upper or "GERENTE DE PLANEJAMENTO" in funcao_upper:
            role = "apoio planejamento"
        elif "GERENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "SUPERINTENDENTE DE OPERACAO" in funcao_upper:
            role = "operacao"
        elif "DESENVOLVIMENTO OPERACIONAL" in funcao_upper:
            role = "adm"
    if not role:
        return RedirectResponse("/register?erro=Função não autorizada para cadastro.", status_code=303)
    hashed_password = pwd_context.hash(password)
    await save_user_bd(username, hashed_password, role)
    return RedirectResponse("/login?msg=Usuário cadastrado com sucesso!", status_code=303)

@router.get("/matriz/operacao")
async def matriz_page(request: Request):
    # logged_in = request.cookies.get(SESSION_COOKIE)
    # if not logged_in or logged_in != "true":
    #     return RedirectResponse("/login", status_code=303)
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["operacao"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    lista_atributos = await get_atributos_matricula(username)
    atributos = sorted(lista_atributos, key=lambda item: item.get('atributo') or '')
    names = await get_names()
    name = names.get(username)
    matrizes_alteradas = await get_matrizes_alteradas_apoio(name)
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    return templates.TemplateResponse("indexOperacao.html", {
        "request": request,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area,
        "matrizes_alteradas": matrizes_alteradas
    })

@router.get("/matriz/apoio")
async def index_apoio(request: Request):
    # logged_in = request.cookies.get(SESSION_COOKIE)
    # if not logged_in or logged_in != "true":
    #     return RedirectResponse("/login", status_code=303)
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    atributos = await get_atributos_apoio(area)
    return templates.TemplateResponse("indexApoio.html", {
        "request": request,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role": user.get("role"),
        "area": area
    })

@router.get("/matriz/apoio/cadastro")
async def index_apoio_cadastro(request: Request):
    # logged_in = request.cookies.get(SESSION_COOKIE)
    # if not logged_in or logged_in != "true":
    #     return RedirectResponse("/login", status_code=303)
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento", "adm"])
    username = request.cookies.get("username")
    indicadores = await get_indicadores()
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "qualidade"
    elif "planejamento" in funcao.lower():
        area = "planejamento"
    atributos = await get_all_atributos_cadastro_apoio(area)
    funcao = await get_funcao(username)
    return templates.TemplateResponse("indexApoioCadastro.html", {
        "request": request,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area
    })

@router.get("/matriz/adm")
async def index_adm(request: Request):
    # logged_in = request.cookies.get(SESSION_COOKIE)
    # if not logged_in or logged_in != "true":
    #     return RedirectResponse("/login", status_code=303)
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["adm"])
    username = request.cookies.get("username")
    
    indicadores = await get_indicadores()
    atributos = await get_atributos_adm()
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    return templates.TemplateResponse("indexAdm.html", {
        "request": request,
        "indicadores": indicadores,
        "username": username,
        "atributos": atributos,
        "role_": user.get("role"),
        "area": area
    })

@router.get("/matriz/adm/acordo")
async def index_adm_acordo(request: Request):
    # logged_in = request.cookies.get(SESSION_COOKIE)
    # if not logged_in or logged_in != "true":
    #     return RedirectResponse("/login", status_code=303)
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["adm"])
    username = request.cookies.get("username")
    print(username)
    indicadores = await get_indicadores()
    gerentes = await get_gerentes()
    operacoes = await get_operacoes()
    return templates.TemplateResponse("indexAdmAcordo.html", {
        "request": request,
        "indicadores": indicadores,
        "username": username,
        "role_": user.get("role"),
        "gerentes": gerentes,
        "operacoes": operacoes,
    })

@router.get("/partials/atributos-smart-adm", response_class=HTMLResponse)
async def partial_atributos_smart_adm(request: Request):
    atributos_smart = await get_atributos_adm()
    return templates.TemplateResponse(
        "_atributos_smart.html",
        {"request": request, "atributos": atributos_smart}
    )

@router.get("/partials/atributos-smart-apoio", response_class=HTMLResponse)
async def partial_atributos_smart_apoio(request: Request):
    user = await get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=303)
    _check_role_or_forbid(user, ["apoio qualidade", "apoio planejamento"])
    username = request.cookies.get("username")
    area = None
    funcao = await get_funcao(username)
    if "qualidade" in funcao.lower():
        area = "Qualidade"
    elif "planejamento" in funcao.lower():
        area = "Planejamento"
    atributos_smart = await get_atributos_apoio(area)
    return templates.TemplateResponse(
        "_atributos_smart.html",
        {"request": request, "atributos": atributos_smart}
    )


@router.post("/add", response_class=HTMLResponse)
async def add_registro(
    request: Request,
    nome: str = Form(...),
    meta: str = Form(...),
    moeda: str = Form(...),
    criterio_final: Optional[str] = Form(None),
    tipo_faturamento: str = Form(...),
    escala: str = Form(...),
    acumulado: str = Form(...),
    tipo_matriz: str = Form(...),
    esquema_acumulado: str = Form(...),
    descricao: Optional[str] = Form(None),
    ativo: Optional[str] = Form(None),
    chamado: Optional[str] = Form(None),
    atributo: str = Form(...),
    tipo_indicador: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    periodo: str = Form(...),
    gerente: str = Form(...),
    ):
    registros = await load_registros(request)
    novo_id = str(uuid.uuid4())
    fact = await get_factibilidade(atributo, nome.split(" - ")[0])
    novo = {
        "id": novo_id,
        "atributo": atributo, "id_nome_indicador": nome, "meta_sugerida": fact[0]["metasugerida"] if fact else '', "resultado": fact[0]["resultado"] if fact else '', "atingimento": fact[0]["atingimento"] if fact else '',
        "meta": meta, "moedas": moeda,"tipo_indicador": tipo_indicador,"acumulado": acumulado,"esquema_acumulado": esquema_acumulado,
        "tipo_matriz": tipo_matriz,"data_inicio": data_inicio,"data_fim": data_fim,"periodo": periodo,"escala": escala,"tipo_de_faturamento": tipo_faturamento,
        "descricao": descricao or '',"ativo": ativo or "","chamado" or '': chamado,"criterio": criterio_final, "gerente": gerente,
        "possui_dmm": 'Não',"dmm": ''
    }
    if not atributo or not nome or not meta or not moeda or not data_inicio or not data_fim or not escala or not tipo_faturamento or not criterio_final:  
        raise HTTPException(
            status_code=422,
            detail="Preencha todos os campos obrigatórios!"
    )
    if len(registros) > 0:
        periodo_registros = registros[0].get("periodo")
        if periodo != periodo_registros:
            raise HTTPException(
                status_code=422,
                detail="Você tentou enviar um indicador com periodo diferente do que já está na tabela! Vá para a pana pagina inicial e relecione o mesmo periodo da tabela ."
            )

    registros.append(novo)
    await save_registros(request, registros)
    html_content = templates.TemplateResponse(
    "_registro.html", 
    {"request": request, "registros": registros} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "Novo registro adicionado com sucesso!"}'
    return response

@router.get("/pesquisar_mes", response_class=HTMLResponse)
async def pesquisar_mes(request: Request,
    atributo: str | None = Query(...),
    mes: str = Query(...)):
    try:
        registros = []

        if (atributo == "" or not atributo):
            raise HTTPException(
                status_code=422,
                detail="Selecione um atributo primeiro!"
            )

        current_page = request.headers.get("hx-current-url", "desconhecido")
        path = urlparse(current_page).path.lower()

        username = request.cookies.get("username", "anon")

        funcao = await get_funcao(username)
        area = None

        if funcao:
            if "qualidade" in funcao.lower():
                area = "Qualidade"
            elif "planejamento" in funcao.lower():
                area = "Planejamento"

        if "cadastro" in path:
            page = "cadastro"
            show_das = None
        else:
            page = "demais"
            show_das = True

        show_checkbox = True
        if mes == "m+1":
            if ("/matriz/apoio" not in path) and ("/matriz/adm" not in path):
                show_checkbox = False

        registros = await query_mes(atributo, username, page, area, mes)

        registros = [
            dic for dic in registros
            if dic.get("id_nome_indicador").lower() != "48 - presença"
        ]


        for dic in registros:
            if isinstance(dic.get("id_nome_indicador"), str) and \
               dic.get("id_nome_indicador").lower() == "901 - % disponibilidade":
                dic["meta_sugerida"] = 94.0
        response = None
        if "operacao" in funcao.lower():
            response = templates.TemplateResponse("_pesquisaOperacao.html", {
                "request": request,
                "registros": registros,
                "show_checkbox": show_checkbox,
                "show_das": show_das
            })
        else:
            response = templates.TemplateResponse("_pesquisa.html", {
                "request": request,
                "registros": registros,
                "show_checkbox": show_checkbox,
                "show_das": show_das
            })

        if registros:
            response.headers["HX-Trigger"] = json.dumps(
                {"mostrarSucesso": "Pesquisa realizada com sucesso!"},
                ensure_ascii=False
            )
        else:
            response.headers["HX-Trigger"] = json.dumps(
                {"mostrarSucesso": "sem_resultados"},
                ensure_ascii=False
            )

        return response

    except Exception as e:
        return Response(
            content=f"{str(e)}",
            status_code=422
        )

@router.get("/sentry-debug")
async def trigger_error():
    division_by_zero = 1 / 0

@router.get("/all_atributes_operacao", response_class=HTMLResponse)
async def all_atributes_operacao(request: Request, tipo_pesquisa: str = Query(...)):
    registros = []
    current_page = request.headers.get("hx-current-url", "desconhecido")
    username = request.cookies.get("username", "anon")
    atributos = await get_atributos_matricula(username)

    atributos_format = " ,".join(f"'{a["atributo"]}'" for a in atributos)
    print(atributos_format)
    
    registros = await get_atributos_gerente(tipo_pesquisa, atributos_format, username)

    for dic in registros:
        if dic.get("id_nome_indicador").lower() == "48 - presença":
            registros.remove(dic)

    path = urlparse(current_page).path.lower()
    show_das = None
    if "cadastro" in path:
        show_das = None
    else:
        show_das = True
    html_content = templates.TemplateResponse(
    "_pesquisaOperacao.html", 
    {"request": request, "registros": registros, "show_checkbox": False, "show_das": show_das}
    )
    response = Response(content=html_content.body, media_type="text/html")
    if len(registros) > 0:
        response.headers["HX-Trigger"] = json.dumps(
            {"mostrarSucesso": "Pesquisa realizada com sucesso!"},
            ensure_ascii=False
        )
    else:
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "sem_resultados"}'
    return response

@router.post("/submit_table", response_class=HTMLResponse)
async def submit_table(request: Request):
    require_htmx(request)
    validate_origin(request)
    form = await request.form()
    escala = form.get("escala_submit")
    registros = await load_registros(request)
    if (not escala or escala == "") and registros[0].get("escala") == "":
        raise HTTPException(status_code=422, detail="Selecione uma escala!")
    if not registros:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": "Nenhum registro para submeter."}
            })
        })
    user = await get_current_user(request)
    role = user.get("role") if user else "unknown"
    username = request.cookies.get("username", "anon")
    por_atributo, por_indicador = preprocess_registros(registros)
    periodo = registros[0]["periodo"]
    check_duplicity = await check_atribute_and_periodo_bd(por_atributo, periodo)
    if len(check_duplicity) > 0:
        raise HTTPException(status_code=422, detail="Você está tentando submeter uma matriz que já foi submetida. Se deseja alterar uma matriz, gentileza utilizar a opção 'Alterar uma Matriz' no painel superior.")
    
    num_atendentes = await get_num_atendentes(registros[0]["atributo"]) if "opera" in registros[0]["tipo_matriz"].lower() else None
    if "opera" in registros[0]["tipo_matriz"].lower():
        if num_atendentes == 0 or num_atendentes == '0':
            return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": "Não é possível submeter a matriz, pois o atributo selecionado não possui nenhum atendente de nível 1."}
                })
            })
    results = None
    try:
        results = await validation_submit_table(registros, username, por_indicador, role)
    except Exception as e:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": f"Erro Inesperado: {e}."}
                })
            })
    if isinstance(results, str):
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": results}
                })
            })      
    try:                
        await save_registros_bd(results, username, escala)
    except Exception:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": "Erro inesperado ao salvar matriz no banco de dados."}
                })
            }) 
    response = Response(
        content="",
        status_code=status.HTTP_200_OK,
        media_type="text/html"
    )
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "Tabela submetida com sucesso"}' 
    return response

@router.post("/duplicate_search_results", response_class=HTMLResponse)
async def duplicate_search_results(
    request: Request,
    atributo: str = Form(...),
    tipo_pesquisa: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...),
    periodo: str = Form(...),
    registro_ids: List[str] = Form([], alias="registro_ids")
):
    try:
        if not data_inicio or not data_fim or not periodo:
            raise HTTPException(
                status_code=422,
                detail="Selecione as datas de início e fim antes de duplicar!"
            )

        if not registro_ids:
            raise HTTPException(
                status_code=422,
                detail="Selecione pelo menos um registro para duplicar."
            )
        
        if not atributo:
            raise HTTPException(
                status_code=422,
                detail="Selecione o atributo antes de duplicar."
            )

        current_page = request.headers.get("hx-current-url", "desconhecido").lower()
        path = urlparse(current_page).path.lower()

        page = "cadastro" if "cadastro" in path else "demais"
        cache_key = generate_cache_key(1, tipo_pesquisa, atributo, page)

        registros_da_pesquisa = await get_from_cache(cache_key)

        if not registros_da_pesquisa:
            raise HTTPException(
                status_code=422,
                detail="Nenhum resultado de pesquisa encontrado. Refaça a pesquisa antes de duplicar."
            )

        ids_selecionados = set(registro_ids)

        registros_a_duplicar = [
            r for r in registros_da_pesquisa
            if str(r.get("id")) in ids_selecionados
        ]

        if not registros_a_duplicar:
            raise HTTPException(
                status_code=422,
                detail="Os registros selecionados não foram encontrados."
            )

        registros_atuais = await load_registros(request)

        for reg in registros_atuais:
            if periodo != reg.get("periodo"):
                raise HTTPException(
                    status_code=422,
                    detail="Você está tentando duplicar dados com um periodo diferente do que está na tabela matriz atualmente."
                )

        for registro in registros_a_duplicar:
            if registro.get("id_nome_indicador").lower() == "48 - presença":
                continue

            copia = registro.copy()
            copia["id"] = str(uuid.uuid4())
            copia["data_inicio"] = data_inicio
            copia["data_fim"] = data_fim
            copia["periodo"] = periodo
            copia["dmm"] = ''
            copia["possui_dmm"] = 'Não'
            copia["ativo"] = 0

            registros_atuais.append(copia)

        await save_registros(request, registros_atuais)

        html_content = templates.TemplateResponse(
            "_registro.html",
            {"request": request, "registros": registros_atuais}
        )

        response = Response(content=html_content.body, media_type="text/html")
        response.headers["HX-Trigger"] = '{"mostrarSucesso": "Registros duplicados com sucesso!"}'
        return response

    except HTTPException:
        raise

    except Exception as e:
        return Response(
            content=f"Erro inesperado ao duplicar registros. ({str(e)})",
            status_code=422
        )


@router.post("/update_registro/{registro_id}/{campo}", response_class=HTMLResponse)
async def update_registro(request: Request, registro_id: str, campo: str, novo_valor: str = Form(..., alias="value")):
    require_htmx(request)
    validate_origin(request)
    user = await get_current_user(request)
    await _check_registro_scope(request, registro_id, user)
    registros = await load_registros(request)
    registro_encontrado = None
    for reg in registros:
        if str(reg.get("id")) == registro_id:
            registro_encontrado = reg
            break 
    if not registro_encontrado:
        return Response(status_code=404, content=f"Registro ID {registro_id} não encontrado.")
    if campo not in ["meta", "moeda", "ativo"]: 
        return Response(status_code=400, content="Campo inválido para edição.")
    if campo == 'ativo':
        _check_role_or_forbid(user, ["adm"])
    valor_limpo = novo_valor.strip()
    valor_processado = valor_limpo 
    tipo_indicador = registro_encontrado.get("tipo_indicador")
    try:
        if campo == "moeda":
            if valor_limpo == '':
                valor_processado = 0
            else:
                valor_processado = int(valor_limpo.replace(',', '.'))
        if campo == "ativo":
            if valor_limpo == '':
                valor_processado = 0
            else:
                valor_processado = int(valor_limpo)
        elif tipo_indicador in ["PERCENTUAL"] and campo != "moeda":
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["INTEIRO"] and campo != "moeda":
            int(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["DECIMAL"] and campo != "moeda":
            float(valor_limpo.replace(',', '.'))
        elif tipo_indicador in ["HORA"] and campo != "moeda":
            partes = valor_limpo.split(":")
            if len(partes) < 3:
                return Response(status_code=404, content=f"Hora inválida: {novo_valor}.")
    except ValueError:
        error_message = f"Valor inválido para o campo {campo}."
        response = Response(content=f'{registro_encontrado.get(campo) or ""}', status_code=400)
        response.headers["HX-Retarget"] = "#mensagens-registros"
        response.headers["HX-Reswap"] = "innerHTML"
        response.headers["HX-Trigger"] = f'{{"mostrarErro": "{error_message}"}}'
        return response
    if campo == 'moeda':
        campo = 'moedas'
    registro_encontrado[campo] = valor_processado 
    await save_registros(request, registros)
    return f'{registro_encontrado.get(campo) or ""}'

@router.get("/edit_campo/{registro_id}/{campo}", response_class=HTMLResponse)
async def edit_campo_get(request: Request, registro_id: str, campo: str):
    user = await get_current_user(request)
    await _check_registro_scope(request, registro_id, user)
    input_type = "text"
    if campo == "ativo":
        _check_role_or_forbid(user, ["adm"])
        input_type = "number"   
    registros = await load_registros(request)
    valor = ""
    for reg in registros:
        if str(reg.get("id")) == registro_id:
            valor = reg.get(campo)
            break     
    safe_valor = escape(str(valor)) if valor is not None else ""
    return f"""
    <td hx-trigger="dblclick" hx-get="/edit_campo/{registro_id}/{campo}" hx-target="this" hx-swap="outerHTML">
        <form hx-post="/update_registro/{registro_id}/{campo}" hx-target="this" hx-swap="outerHTML">
            <input name="value" 
                    type="{input_type}" 
                    value="{safe_valor}"
                    class="in-place-edit-input" 
                    autofocus
                    hx-trigger="focusout, keyup[enter]" 
                    hx-confirm="Confirma a alteração do campo {campo}?">
        </form>
    </td>
    """

@router.post("/processar_acordo", response_class=HTMLResponse)
async def processar_acordo(
    request: Request, 
):
    require_htmx(request)
    validate_origin(request)
    form_data = await request.form()
    tipo = next((v for v in form_data.getlist("tipo_pesquisa") if v), None)
    atributo = next((v for v in form_data.getlist("atributo") if v), None)
    page = next((v for v in form_data.getlist("page") if v), None)
    status_acao = next((v for v in form_data.getlist("status_acao") if v), None)
    user = await get_current_user(request)
    _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"])
    role = user.get("role", "default").lower().strip()
    cache_key = generate_cache_key(1, tipo, atributo, page)
    username = user.get("usuario", "anon")
    data = datetime.now().strftime("%Y-%m-%d")
    print(cache_key)
    

    if not cache_key:
        raise HTTPException(
            status_code=422,
            detail="Faça uma pesquisa antes de dar acordo ou não acordo."
        )

    status_acao = status_acao.lower().strip()
    cache_key = cache_key.strip()

    try:
        registros_pesquisa = await get_from_cache(cache_key)
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=422,
            detail="Erro ao acessar o cache da pesquisa."
        )
    if not registros_pesquisa:
        raise HTTPException(
            status_code=422,
            detail="Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa."
        )
    try:
        if len(registros_pesquisa) > 0:
            if int(registros_pesquisa[0].get("ativo", 0)) != 0:
                raise HTTPException(
                    status_code=422,
                    detail="Não é possível dar acordo ou não acordo para registros que já passaram pelo DA da Exop."
                )
        else:
            raise HTTPException(
                status_code=422,
                detail="Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa."
            )
    except ValueError:
        pass

    updates_a_executar = []
    trava_da_exop = []
    moedas_v = 0

    for r in registros_pesquisa:
        atributo = str(r.get("atributo", "")).strip()
        periodo = str(r.get("periodo", "")).strip()
        moedas_v += int(r.get("moedas", 0))

        updates_a_executar.append((atributo, periodo))
        trava_da_exop.append(r)
    if moedas_v != 30 and moedas_v != 35:
        raise HTTPException(
            status_code=422,
            detail="A soma das moedas da matriz deve ser 30 ou 35 para realizar a ação."
        )

    if updates_a_executar:
        try:
            if role == "adm" and trava_da_exop[0].get("tipo_matriz").lower().strip() == "operacional":
                for dic in trava_da_exop:
                    qualidade = int(dic.get("da_qualidade", 0))
                    planejamento = int(dic.get("da_planejamento", 0))
                    if (qualidade == 0 and planejamento == 0) or (qualidade == 1 and planejamento == 0) or (qualidade == 0 and planejamento == 1):
                        raise HTTPException(
                            status_code=422,
                            detail="Validação da qualidade ou do planejamento está ausente para o atributo selecionado."
                        )
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=f"Não foi possível validar os DA's das areas de apoio. ({e})"
            )            

        try:
            await update_da_adm_apoio(updates_a_executar, role, status_acao, username)
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=f"Erro ao atualizar os registros ({e})."
            )
    registros_copy = copy.deepcopy(registros_pesquisa)
    for r in registros_copy:
        if role == "apoio qualidade":
            r["da_qualidade"] = 1 if status_acao == "acordo" else 2
            r["qualidade"] = username
            r["data_da_qualidade"] = data
        elif role == "apoio planejamento":
            r["da_planejamento"] = 1 if status_acao == "acordo" else 2
            r["planejamento"] = username
            r["data_da_planejamento"] = data
        elif role == "adm":
            r["da_exop"] = 1 if status_acao == "acordo" else 2
            r["exop"] = username
            r["data_da_exop"] = data
    registros_apos_acao = [r for r in registros_copy if r.get("id_nome_indicador", "").lower() != "48 - presença"]
    response = templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True,
            "show_das": True
        }
    )
    response.headers["HX-Trigger"] = json.dumps({
        "mostrarSucesso": {"value": "DA/NA atualizado com sucesso!"},
        "refreshAtributosSmart": True,
    })
    await set_cache(cache_key, registros_copy, CACHE_TTL)
    return response

@router.post("/update_meta_moedas", response_class=HTMLResponse)
async def update_meta_moedas(
    request: Request, 
    registro_ids: List[str] = Form([], alias="registro_ids"),
    meta: str = Form(None, alias="meta_duplicar"),
    moedas: str = Form(None, alias="moedas_duplicar")
    ):
    form_data = await request.form()
    tipo = next((v for v in form_data.getlist("tipo_pesquisa") if v), None)
    atributo = next((v for v in form_data.getlist("atributo") if v), None)
    page = next((v for v in form_data.getlist("page") if v), None)
    cache_key = generate_cache_key(1, tipo, atributo, page)
    require_htmx(request)
    validate_origin(request)
    user = await get_current_user(request)
    username = user.get("usuario")
    role = user.get("role")
    _check_role_or_forbid(user, ["adm", "apoio qualidade", "apoio planejamento"])
    if moedas != "" and moedas != None:
        _check_role_or_forbid(user, ["adm"])
    if not registro_ids:
        raise HTTPException(
            status_code=422,
            detail="Selecione pelo menos um registro para alterar."
        )
    if not meta and not moedas:
        raise HTTPException(
            status_code=422,
            detail="Preencha pelo menos um dos campos para efetuar a alteração."
        )
    registros_pesquisa = await get_from_cache(cache_key)
    if len(registro_ids) > 1:
        raise HTTPException(
            status_code=422,
            detail="Selecione apenas um campo para alterar."
        )
    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    try:
        if len(registros_pesquisa) > 0:
            if int(registros_pesquisa[0].get("ativo", 0)) != 0 and role != "adm":
                raise HTTPException(
                    status_code=422,
                    detail="Não é possível alterar a meta para uma matriz que já passou pelo DA da Exop."
                )
    except ValueError:
        pass
    ids_selecionados = set(registro_ids)
    updates_a_executar = []
    registros_selecionados = []
    meta_v = None
    moedas_v = None
    dmm_v = None
    for r in registros_pesquisa:
        if str(r.get("id")) in ids_selecionados:
            erro = await validation_meta_moedas(r, meta, moedas, role)
            if erro:
                raise HTTPException(status_code=422, detail=erro)
            atributo = r.get("atributo")
            id_nome_indicador = r.get("id_nome_indicador") 
            periodo = r.get("periodo")
            data_inicio = r.get("data_inicio")
            meta_v = meta if meta != None and meta != "" else r.get("meta")
            moedas_v = moedas if moedas != None and moedas != "" else r.get("moedas")
            dmm_v = r.get("dmm")
            updates_a_executar.append((atributo, periodo, id_nome_indicador, data_inicio)) 
            registros_selecionados.append(r)
    if updates_a_executar:
        await update_meta_moedas_bd(updates_a_executar, meta, moedas, role, username, registros_pesquisa[0]["ativo"])
        await insert_log_auditoria(registros_selecionados, meta_v, moedas_v, dmm_v, username)
    registros_copy = copy.deepcopy(registros_pesquisa)
    for r in registros_copy:
        if str(r.get("id")) in ids_selecionados:
            if meta != "" and meta is not None:
                r["meta"] = meta
            if moedas != "" and moedas is not None:
                r["moedas"] = moedas
    registros_apos_acao = [dic for dic in registros_copy if dic["id_nome_indicador"].lower() != "48 - presença"]
    await set_cache(cache_key, registros_apos_acao, CACHE_TTL)
    response = templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True,
            "show_das": True
        }
    )
    response.headers["HX-Trigger"] = json.dumps({
            "mostrarSucesso": {"value": f"Meta/Moedas alterados com sucesso!"}
        })

    return response

@router.post("/update_dmm", response_class=HTMLResponse)
async def update_dmm(
    request: Request, 
):
    user = await get_current_user(request)
    username = user.get("usuario")
    _check_role_or_forbid(user, ["adm", "apoio planejamento"])
    form_data = await request.form()
    tipo = next((v for v in form_data.getlist("tipo_pesquisa") if v), None)
    atributo = next((v for v in form_data.getlist("atributo") if v), None)
    page = next((v for v in form_data.getlist("page") if v), None)
    dmm = (form_data.get("dmm_apoio") or "").strip()
    cache_key = generate_cache_key(1, tipo, atributo, page)
    erro = await validation_dmm(dmm)
    if erro:
        raise HTTPException(status_code=422, detail=erro)
    if not dmm:
        raise HTTPException(
            status_code=422,
            detail="Coloque exatamente 5 dmms para efetuar a alteração."
        )
    registros_pesquisa = await get_from_cache(cache_key)
    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="Cache de pesquisa não encontrado ou expirado. Refaça a pesquisa.")
    try:
        if len(registros_pesquisa) > 0:
            if int(registros_pesquisa[0].get("ativo", 0)) != 0 and user.get("role") != "adm":
                raise HTTPException(status_code=422, detail="Não é possivel alterar o DMM de uma matriz que já tem DA da exop.")
    except ValueError:
        pass
    await update_dmm_bd(registros_pesquisa[0]["atributo"], registros_pesquisa[0]["periodo"], dmm)
    await insert_log_auditoria(registros_pesquisa, None, None, dmm, username)
    registros_copy = copy.deepcopy(registros_pesquisa)
    try:
        for r in registros_copy:
            r["possui_dmm"] = "SIM"
            r["dmm"] = dmm
    except Exception:
        raise HTTPException(status_code=422, detail=f"Erro ao recuperar a matriz. Refaça a pesquisa e tente novamente.")
    registros_apos_acao = [dic for dic in registros_copy if dic["id_nome_indicador"].lower() != "48 - presença"]
    await set_cache(cache_key, registros_apos_acao, CACHE_TTL)
    response =  templates.TemplateResponse(
        "_pesquisa.html", 
        {
            "request": request, 
            "registros": registros_apos_acao,
            "show_checkbox": True,
            "show_das": True
        }
    )
    response.headers["HX-Trigger"] = json.dumps({
            "mostrarSucesso": {"value": f"DMM alterado com sucesso!"}
        })

    return response


@router.post("/clear_registros", response_class=HTMLResponse)
async def clear_registros_route(request: Request):
    """
    Limpa os registros atuais do usuário no cache (session).
    Retorna uma string vazia para o HTMX limpar o elemento alvo no frontend.
    """
    try:
        await save_registros(request, []) 
        return ""
    except Exception as e:
        print(f"Erro ao limpar registros: {e}")
        return HTMLResponse(content=f"<div style='color: red;'>Erro interno ao limpar os registros: {e}</div>", status_code=422)
    
@router.get("/export_table")
async def export_table(request: Request):
    form_data = request.query_params
    print(form_data)
    tipo = next((v for v in form_data.getlist("tipo_pesquisa") if v), None)
    atributo = next((v for v in form_data.getlist("atributo") if v), None)
    page = next((v for v in form_data.getlist("page") if v), None)
    modo = next((v for v in form_data.getlist("modo") if v), None)
    cache_key = None
    print(tipo, atributo, page, modo)
    if modo == "pesquisar_mes":
        cache_key = generate_cache_key(1, tipo, atributo, page)
    elif modo == "all_atributes_operacao":
        username = request.cookies.get("username", "anon")
        cache_key = generate_cache_key(2, tipo, None, None, username)
    if not cache_key:
        return HTTPException(status_code=422, detail="Parâmetros insuficientes para determinar o cache a ser exportado.")
    registros_pesquisa = await get_from_cache(cache_key)

    if not registros_pesquisa:
        raise HTTPException(status_code=422, detail="Nenhum resultado de pesquisa encontrado no cache. Execute a pesquisa primeiro.")

    colunas = EXPECTED_COLUMNS
    df = pd.DataFrame(registros_pesquisa)
    final_cols = [c for c in colunas if c in df.columns]
    df = df[final_cols]
    colunas_to_drop = ['qualidade', 'da_qualidade', 'data_da_qualidade', 
        'planejamento', 'da_planejamento', 'data_da_planejamento']
    if "apoio" in tipo:
        df = df.drop(columns=colunas_to_drop)
    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Pesquisa', engine='openpyxl')
    output.seek(0)

    filename = f"pesquisa_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.get("/export_atributos_sem_matriz")
async def export_atributos_sem_matriz(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    registros_pesquisa = await get_matrizes_nao_cadastradas()

    df = pd.DataFrame(registros_pesquisa)

    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Matrizes_Nao_Cadastradas', engine='openpyxl')
    output.seek(0)

    filename = f"matrizes_nao_cadastradas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.get("/export_pendencias_apoio")
async def export_atributos_sem_matriz(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    registros_pesquisa = await get_pendencias_apoio()

    df = pd.DataFrame(registros_pesquisa)

    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Matrizes_Nao_Cadastradas', engine='openpyxl')
    output.seek(0)

    filename = f"pendencias_apoio{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.get("/export_alteracoes")
async def export_atributos_sem_matriz(request: Request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Sessão inválida")

    registros_pesquisa = await get_all_alterations()

    df = pd.DataFrame(registros_pesquisa)

    output = BytesIO()
    df.to_excel(output, index=False, sheet_name='Matrizes_Alteradas', engine='openpyxl')
    output.seek(0)

    filename = f"matrizes_alteradas{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={'Content-Disposition': f'attachment; filename="{filename}"'}
    )

@router.post("/upload_excel", response_class=HTMLResponse)
async def upload_excel(request: Request, file: UploadFile = File(...)):
    username = request.cookies.get("username")
    user = await get_current_user(request)
    role = user.get("role", "default")
    _check_role_or_forbid(user, ["adm"])
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Envie um arquivo Excel (.xlsx ou .xls)."}
            })
        })
    try:
        content = await file.read()
        df = await run_in_threadpool(pd.read_excel, BytesIO(content))
    except Exception as e:
        await file.close()
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Erro ao ler o arquivo Excel: {e}."}
            })
        })
    finally:
        await file.close()
    if df.empty:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"O arquivo Excel está vazio."}
            })
        })
    df_cols = [c.strip() for c in df.columns]
    if df_cols != EXPECTED_COLUMNS_IMPORT:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"As colunas do arquivo não correspondem ao modelo esperado.<br>\nEsperado: {EXPECTED_COLUMNS_IMPORT}<br>Recebido: {df_cols}."}
            })
        })
    for col in df.columns:
        df[col] = df[col].apply(clean_value)
    date_cols = [c for c in df.columns if "data" in c.lower()]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            df[col] = ""
    df = df.fillna("")
    for col in ["ativo", "qualidade", "da_qualidade", "planejamento", "da_planejamento", "exop", "da_exop"]:
        if col in df.columns:
            df[col] = df[col].apply(to_int_safe)
    records = df.to_dict(orient="records")
    por_atributo, por_indicador = preprocess_registros(records)
    periodo = records[0]["periodo"]
    check_duplicity = await check_atribute_and_periodo_bd(por_atributo, periodo)
    if len(check_duplicity) > 0:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": f"O atributo {check_duplicity[0]} ja possui matriz para o periodo {periodo}, não sendo possível importar o excel."}
                })
            })
    valid_records = await validation_import_from_excel(records, request)
    valid_submit = await validation_submit_table(records, username, por_indicador, role)
    if valid_records:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": valid_records}
                })
            })
    if isinstance(valid_submit, str):
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": valid_submit}
                })
            })    
    try:
        await import_from_excel(records, username)
    except Exception as e:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": str(e)}
                })
            })
    return Response(
    "",
    headers={
        "HX-Trigger": json.dumps({
            "mostrarSucesso": {"value": f"Upload efetuado com sucesso!"}
        })
    })

@router.post("/replicar_registros", response_class=HTMLResponse)
async def replicar_registros(request: Request, atributos_replicar: list[str] = Form(None)):
    require_htmx(request)
    validate_origin(request)
    user = None
    matricula = None
    role = None
    registros = await load_registros(request)
    por_atributo, por_indicador = preprocess_registros(registros)
    
    if len(registros) == 0 or not isinstance(registros, list) or not registros:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Não há registros carregados no cache para replicar."}
            })
        })
    
    periodo = registros[0]["periodo"]

    if not atributos_replicar:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Nenhum atributo selecionado para replicação."}
            })
        })
    
    try:
        user = await get_current_user(request)
        _check_role_or_forbid(user, ["operacao"])
        matricula = user.get("usuario")
        role = user.get("role", "default")
    except Exception:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Usuário não autenticado."}
            })
        })

    atributos_destino = [a.strip() for a in atributos_replicar if a and a.strip()]
    check_duplicity = await check_atribute_and_periodo_bd(atributos_destino, periodo)
    if len(check_duplicity) > 0:
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": f"O atributo {check_duplicity[0]} ja possui matriz para o periodo {periodo}, não sendo possível replicar."}
                })
            })
    atributo_atual = registros[0].get("atributo")
    novos_registros = []
    for destino in atributos_destino:
        for r in registros:
            novo = dict(r)
            novo["atributo"] = destino
            novo["submetido_por"] = matricula
            novo["data_submetido_por"] = datetime.now().strftime("%Y-%m-%d")
            novo["qualidade"] = 0
            novo["da_qualidade"] = 0
            novo["data_da_qualidade"] = ''
            novo["planejamento"] = 0
            novo["da_planejamento"] = 0
            novo["data_da_planejamento"] = ''
            novo["exop"] = 0
            novo["da_exop"] = 0
            novo["data_da_exop"] = ''
            if "id" in novo:
                novo["id"] = ""
            novos_registros.append(novo)
    results = await validation_submit_table(novos_registros, matricula, por_indicador, role)
    if isinstance(results, str):
        return Response(
            "",
            headers={
                "HX-Trigger": json.dumps({
                    "mostrarErro": {"value": results}
                })
            })
 
    if not novos_registros:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Nenhum registro válido para replicar."}
            })
        })

    try:
        await import_from_excel(novos_registros, matricula)
    except Exception as e:
        return Response(
        "",
        headers={
            "HX-Trigger": json.dumps({
                "mostrarErro": {"value": f"Erro ao inserir registros no banco: {e}."}
            })
        })

    return Response(
    "",
    headers={
        "HX-Trigger": json.dumps({
            "mostrarSucesso": {"value": f"{len(registros)} registros replicados do atributo {atributo_atual} para {len(atributos_destino)} atributo(s)."}
        })
    })