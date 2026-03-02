from datetime import datetime
import uuid
from app.cache import get_from_cache, set_cache
from app.database import get_db_connection
import asyncio 
from datetime import timedelta

SISTEMA_MATRIZ = 'Robbyson.dbo.sistema_matriz'
HOMINUM = 'rlt.hmn (nolock)'
CACHE_TTL_PADRAO = timedelta(minutes=5)
CACHE_TTL_12H = timedelta(hours=12)

async def get_user_bd(username):
    cache_key = "user: " + username
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from Robbyson.dbo.acessos_sistema_matriz (nolock) where username = ?
            """,(username))
            resultados = [{"username": i[0], "password": i[1], "role": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if len(resultados) == 1:
        return resultados[0]
    await set_cache(cache_key, None)
    return None

async def save_user_bd(username, hashed_password, role):
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            insert into Robbyson.dbo.acessos_sistema_matriz (username, password, role) values ('{username}', '{hashed_password}', '{role}')
            """)
            cur.commit()
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def save_registros_bd(registros, username, escala):
    data = datetime.now()
    username_val = str(username) if username is not None else None
    data_val = data

    NUM_COLUNAS_DADOS = 22 
    NUM_COLUNAS_ADICIONAIS = 2 
    NUM_COLUNAS_VAZIAS = 10
    TOTAL_COLUNAS = NUM_COLUNAS_DADOS + NUM_COLUNAS_ADICIONAIS + NUM_COLUNAS_VAZIAS

    loop = asyncio.get_event_loop()

    def _sync_db_call():
        all_rows = []

        for i in registros:
            meta_val = str(i.get('meta')) if i.get('meta') is not None else ''
            is_presence = True if i.get('id_nome_indicador').lower() == r"48 - presença" else False
            area = i.get('area')
            responsavel = i.get('responsavel')

            row_data = [
                i.get('atributo'),
                i.get('id_nome_indicador'),
                meta_val,  
                i.get('moedas'),
                i.get('tipo_indicador'),
                i.get('acumulado'),
                i.get('esquema_acumulado'),
                i.get('tipo_matriz'),
                i.get('data_inicio'),
                i.get('data_fim'),
                i.get('periodo'),
                escala if escala is not None and escala != '' else i.get('escala'),
                i.get('tipo_de_faturamento'),
                i.get('descricao'),
                i.get('ativo'),
                i.get('chamado'),
                i.get('criterio'),
                area if area is not None else '',
                responsavel if responsavel is not None else '',
                i.get('gerente'),
                i.get('possui_dmm'),
                i.get('dmm'),
                username_val,
                data_val,
                '', 3 if is_presence else '', '', '', 3 if is_presence else '', '', '', '', '', i.get('superintendente') or ''
            ]
            all_rows.append(tuple(row_data))

        if not all_rows:
            return

        colunas = (
            "atributo,id_nome_indicador,meta,moedas,tipo_indicador,acumulado,esquema_acumulado,"
            "tipo_matriz,data_inicio,data_fim,periodo,escala,tipo_de_faturamento,descricao,ativo,"
            "chamado,criterio,area,responsavel,gerente,possui_dmm,dmm,submetido_por,data_submetido_por,"
            "qualidade,da_qualidade,data_da_qualidade,planejamento,da_planejamento,data_da_planejamento,"
            "exop,da_exop,data_da_exop,superintendente"
        )

        placeholders = ", ".join(["?"] * TOTAL_COLUNAS)

        insert_query = f"""
            INSERT INTO {SISTEMA_MATRIZ} ({colunas})
            VALUES ({placeholders})
        """

        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.executemany(insert_query, all_rows)
            conn.commit()
            cur.close()

    await loop.run_in_executor(None, _sync_db_call)

async def import_from_excel(registros, username):
    data = datetime.now()
    data_formatada = data.strftime("%Y-%m-%d")
    TOTAL_COLUNAS = 34
    loop = asyncio.get_event_loop()

    def _sync_db_call():
        all_rows = []

        for i in registros:
            meta_val = str(i.get('meta')) if i.get('meta') is not None else ''
            area = i.get('area')
            responsavel = i.get('responsavel')

            row_data = [
                i.get('atributo').strip(),
                i.get('id_nome_indicador'),
                meta_val,  
                i.get('moedas'),
                i.get('tipo_indicador'),
                i.get('acumulado'),
                i.get('esquema_acumulado'),
                i.get('tipo_matriz'),
                i.get('data_inicio'),
                i.get('data_fim'),
                i.get('periodo'),
                i.get('escala'),
                i.get('tipo_de_faturamento'),
                i.get('descricao'),
                0,
                i.get('chamado'),
                i.get('criterio'),
                area if area is not None else '',
                responsavel if responsavel is not None else '',
                i.get('gerente'),
                i.get('possui_dmm'),
                i.get('dmm'),
                username,
                data_formatada,
                0,
                0,
                '',
                0,
                0,
                '',
                0,
                0,
                '',
                i.get('superintendente') or ''
            ]
            all_rows.append(tuple(row_data))

        if not all_rows:
            return

        colunas = (
            "atributo,id_nome_indicador,meta,moedas,tipo_indicador,acumulado,esquema_acumulado,"
            "tipo_matriz,data_inicio,data_fim,periodo,escala,tipo_de_faturamento,descricao,ativo,"
            "chamado,criterio,area,responsavel,gerente,possui_dmm,dmm,submetido_por,data_submetido_por,"
            "qualidade,da_qualidade,data_da_qualidade,planejamento,da_planejamento,data_da_planejamento,"
            "exop,da_exop,data_da_exop,superintendente"
        )

        placeholders = ", ".join(["?"] * TOTAL_COLUNAS)

        insert_query = f"""
            INSERT INTO {SISTEMA_MATRIZ} ({colunas})
            VALUES ({placeholders})
        """
        erro = None
        with get_db_connection() as conn:
            cur = conn.cursor()
            try:
                cur.executemany(insert_query, all_rows)
                conn.commit()
            except Exception as e:
                erro = str(e)
                conn.rollback()
            finally:
                cur.close()
        if erro:
            raise Exception(f"Erro ao importar dados: {erro}")

    await loop.run_in_executor(None, _sync_db_call)

async def check_atribute_and_periodo_bd(dict_of_atributes, periodo):
    atributos = None
    if isinstance(dict_of_atributes, dict):
        atributos = tuple(dict_of_atributes.keys())
    else:
        atributos = tuple(dict_of_atributes)
    loop = asyncio.get_event_loop()

    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()

            placeholders = ",".join("?" for _ in atributos)

            validation_query = f"""
                SELECT distinct atributo
                FROM {SISTEMA_MATRIZ} WITH (NOLOCK)
                WHERE atributo IN ({placeholders})
                  AND periodo = ?
            """

            cur.execute(validation_query, (*atributos, periodo))
            result = cur.fetchall()
            cur.close()
            return result

    return await loop.run_in_executor(None, _sync_db_call)

async def get_all_atributos():
    cache_key = "all_atributos"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct atributo from [robbyson].[rlt].[hmn] (nolock) where (data = convert(date, getdate()-1)) and (atributo is not null) 
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, CACHE_TTL_12H)
    return resultados

async def query_mes(atributo, username, page, area, mes):
    cache_key = f"pesquisa_{mes}:{atributo}:{page}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    tipo_pesquisa_mg = None
    tipo_pesquisa_fef = None
    if mes == 'm0':
        tipo_pesquisa_mg = -1
        tipo_pesquisa_fef = -1
    elif mes == 'm+1':
        tipo_pesquisa_mg = 0
        tipo_pesquisa_fef = -1
    elif mes == 'm1':
        tipo_pesquisa_mg = -2
        tipo_pesquisa_fef = -2
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if page == "demais":
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador (nolock)

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, ?, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop, superintendente
                from {SISTEMA_MATRIZ} mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                AND periodo = dateadd(d,1,eomonth(GETDATE(),?))
                AND ativo in (0, 1, 3)
                order by moedas desc

                drop table #formatos
                drop table #fef
                """,(tipo_pesquisa_fef, atributo, atributo, tipo_pesquisa_mg))
            elif page == "cadastro":
                cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador (nolock)

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, ?, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop, superintendente
                from {SISTEMA_MATRIZ} mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = ?
                WHERE mg.atributo = ?
                and tipo_matriz like 'ADMINISTRA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),?))
                AND ativo in (0, 1, 3)
                order by moedas desc

                drop table #formatos
                drop table #fef
                """,(tipo_pesquisa_fef, atributo, atributo, tipo_pesquisa_mg))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta_sugerida": row[2] or '', "resultado": row[3] or '', "atingimento": row[4] or '', "meta": row[5], "moedas": row[6], "tipo_indicador": row[7] or '', "acumulado": row[8] or '', "esquema_acumulado": row[9] or '',
        "tipo_matriz": row[10] or '', "data_inicio": row[11], "data_fim": row[12], "periodo": row[13], "escala": row[14] or '', "tipo_de_faturamento": row[15] or '', "descricao": row[16] or '', "ativo": row[17], "chamado": row[18] or '',
        "criterio": row[19] or '', "area": row[20] or '', "responsavel": row[21] or '', "gerente": row[22] or '', "possui_dmm": row[23] or '', "dmm": row[24] or '',
        "submetido_por": row[25], "data_submetido_por": row[26], "qualidade": row[27], "da_qualidade": row[28], "data_da_qualidade": row[29],
        "planejamento": row[30], "da_planejamento": row[31], "data_da_planejamento": row[32], "exop": row[33], "da_exop": row[34], "data_da_exop": row[35], "superintendente": row[36], "id": str(uuid.uuid4())
    } for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_PADRAO)
    return registros

async def update_da_adm_apoio(lista_de_updates: list, role, tipo, username): 
    role_defined = None
    tipo_defined = 1 if tipo == 'acordo' else 2
    if role == "apoio qualidade":
        role_defined = "qualidade"
    elif role == "apoio planejamento":
        role_defined = "planejamento"
    elif role == "adm":
        role_defined = "exop"
    else:
        return None
    agora = datetime.now()
    campo_usuario = role_defined
    campo_da = "da_"+role_defined
    campo_data = "data_da_"+role_defined
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            for update_item in lista_de_updates:
                atributo, periodo = update_item
                if role_defined == "exop" and tipo_defined == 1:
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        ativo = 1,
                        {campo_usuario} = ?,
                        {campo_da} = ?,
                        {campo_data} = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ?  
                """, (username, tipo_defined, agora, atributo, periodo,))
                else:
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        {campo_usuario} = ?,
                        {campo_da} = ?,
                        {campo_data} = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ?  
                """, (username, tipo_defined, agora, atributo, periodo,))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def update_dmm_bd(atributo, periodo, dmm): 
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            UPDATE {SISTEMA_MATRIZ}
            SET 
                dmm = ?
            WHERE 
                Atributo = ? AND 
                periodo = ?
            """, (str(dmm), atributo, periodo,))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def insert_log_auditoria(registros_selecionados, meta, moedas, dmm, username): 
    loop = asyncio.get_event_loop()
    agora = datetime.now()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if meta == None and moedas == None:
                cur.execute(f"""
                    INSERT INTO dbo.auditoria_sistema_matriz (atributo, id_nome_indicador, meta_antiga, nova_meta, alterado_por, resultado_m0, gerente, periodo, moeda_antiga, nova_moeda, dmm_antigo, novo_dmm, data_alterado_por, superintendente)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (registros_selecionados[0]["atributo"], '', '', '', username, '', registros_selecionados[0].get("gerente"), registros_selecionados[0]["periodo"], '', '', registros_selecionados[0].get("dmm"), dmm, agora, registros_selecionados[0].get("superintendente")))
            else:
                for registro in registros_selecionados:
                    cur.execute(f"""
                    INSERT INTO dbo.auditoria_sistema_matriz (atributo, id_nome_indicador, meta_antiga, nova_meta, alterado_por, resultado_m0, gerente, periodo, moeda_antiga, nova_moeda, dmm_antigo, novo_dmm, data_alterado_por, superintendente)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (registro["atributo"], registro["id_nome_indicador"], registro["meta"], meta, username, registro.get("resultado"), registro.get("gerente"), registro["periodo"], registro.get("moedas"), moedas, '', '', agora, registro.get("superintendente")))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)

async def update_meta_moedas_bd(lista_de_updates: list, meta, moedas, role, username, ativo): 
    agora = datetime.now()
    role_defined = None
    updated = False
    if role == "apoio qualidade":
        role_defined = "qualidade"
    elif role == "apoio planejamento":
        role_defined = "planejamento"
    elif role == "adm":
        role_defined = "exop"
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            for update_item in lista_de_updates:
                atributo, periodo, id_nome_indicador, data_inicio = update_item
                if meta != '' and meta != None and moedas != '' and moedas != None:
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        meta = ?,
                        moedas = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ? AND
                        data_inicio = ?
                    """, (meta, moedas, atributo, periodo, id_nome_indicador, data_inicio))
                    updated = True
                elif (meta != '' and meta != None) and (moedas == '' or moedas == None):
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        meta = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ? AND
                        data_inicio = ?
                    """, (meta, atributo, periodo, id_nome_indicador, data_inicio))
                    updated = True
                elif (meta == '' or meta == None) and (moedas != '' and moedas != None):
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        moedas = ?
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ? AND
                        data_inicio = ?
                    """, (moedas, atributo, periodo, id_nome_indicador, data_inicio))
                    updated = True
                if role_defined == "exop" and ativo == 1 and updated:
                    cur.execute(f"""
                    UPDATE {SISTEMA_MATRIZ}
                    SET 
                        data_da_exop = ?,
                        ativo = 3
                    WHERE 
                        Atributo = ? AND 
                        periodo = ? AND 
                        id_nome_indicador = ? AND
                        data_inicio = ?
                    """, (datetime.now(), atributo, periodo, id_nome_indicador, data_inicio))
            conn.commit() 
            cur.close()
    await loop.run_in_executor(None, _sync_db_call)



async def get_matrizes_alteradas_apoio(name):
    cache_key = f"matrizes_alteradas_apoio:{name}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached

    loop = asyncio.get_event_loop()

    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT atributo,
                       id_nome_indicador,
                       meta_antiga,
                       nova_meta,
                       alterado_por,
                       resultado_m0,
                       periodo,
                       moeda_antiga,
                        nova_moeda,
                        dmm_antigo,
                        novo_dmm,
                        data_alterado_por,
                        superintendente
                FROM robbyson.dbo.auditoria_sistema_matriz (NOLOCK)
                WHERE (gerente = ? or superintendente = ?)
                and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
                and id_nome_indicador <> ' '
            """, (name, name))

            rows = cur.fetchall()
            cur.close()

            return [
                {
                    "atributo": r[0],
                    "id_nome_indicador": r[1],
                    "meta_antiga": r[2],
                    "meta_nova": r[3],
                    "alterado_por": r[4],
                    "resultado_m0": r[5],
                    "periodo": r[6],
                    "moeda_antiga": r[7],
                    "moeda_nova": r[8],
                    "dmm_antigo": r[9],
                    "dmm_novo": r[10],
                    "data_alterado_por": r[11],
                    "superintendente": r[12]
                }
                for r in rows
            ]

    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, ttl=CACHE_TTL_PADRAO)

    return resultados


async def get_resultados_indicadores_m3(atributo):
    cache_key = f"resultados_indicadores_m3:{atributo}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                set nocount on
                select distinct matricula, atributo
                into #hmn from rlt.hmn (nolock) 
                where data = convert(date, getdate()-1)
                and atributo is not null

                SELECT DISTINCT idindicador
                FROM [Robbyson].[ext].[indicadoresgeral] ig (nolock)
                left join #hmn h on ig.matricula = h.matricula
                where ig.data >= dateadd(d,1,eomonth(GETDATE(),-3))
                and atributo = ?

                drop table #hmn
            """, (atributo,))
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, ttl=CACHE_TTL_12H)
    return resultados

async def get_factibilidade(atributo, id):
    cache_key = f"factibilidade:{atributo}:{id}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador (nolock)

                select concat(fef.id, ' - ', fef.nome_indicador) as id_nome_indicador, 
                case when f.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')
                when f.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when f.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss')
                when f.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on fef.id = f.id_indicador
                where atributo = ?
                and id = ?
                and data = dateadd(d,1,eomonth(GETDATE(),-1))

                drop table #formatos
            """, (atributo,id,))
            resultados = [{"id_nome_indicador": i[0], "metasugerida": i[1], "resultado": i[2], "atingimento": i[3] } for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, CACHE_TTL_12H)
    return resultados

async def get_indicadores():
    cache_key = "indicadores"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT id_indicador, indicador, 
                case when indicador like '%Semanal%' then 'SIM' 
                when indicador like '%Mensal%' then 'NAO' else 'NAO' end as Acumulado,
                case when indicador like '%Semanal%' then 'SEMANAL'
                when indicador like '%Mensal%' then 'MENSAL' else 'DIARIO' end as Esquema_acumulado,
                case when id_formato = 1 then 'INTEIRO'
                when id_formato = 2 then 'DECIMAL'
                when id_formato = 3 then 'PERCENTUAL' 
                when id_formato = 4 then 'HORA' else '' end as Formato
                FROM rby_indicador (nolock)
                WHERE indicador <> 'Descontinuado' 
                AND indicador <> 'INdicador Disponivel'
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    indicadores = [{"id": str(i[0]), "text": i[1].upper(), "acumulado": i[2], "esquema_acumulado": i[3], "formato": str(i[4])} for i in resultados]
    await set_cache(cache_key, indicadores, CACHE_TTL_12H)
    return indicadores

async def get_atributos_adm():
    cache_key = "atributos_adm"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            set nocount on
            select distinct atributo, count(matricula) as atendentes
            into #base_hmn
            from rlt.hmn (nolock) 
            where data = convert(date, getdate()-1)
            and tipohierarquia = 'operação' and nivelhierarquico = 'operacional'
            and funcaorm not like '%analista%' and FuncaoRM not like '%auxiliar%'
            and situacaohominum in ('ativo', 'treinamento')
            and atributo is not null
            group by atributo
            union all
            select distinct atributo, count(matricula) as atendentes
            from rlt.hmn (nolock) 
            where data = convert(date, getdate()-1)
            and atributo is not null
            and ((tipohierarquia = 'ADMINISTRAÇÃO' and nivelhierarquico = 'OPERACIONAL'))
            group by atributo

            ;WITH gerentes_rank AS (
                SELECT 
                    atributo, 
                    CASE 
                        WHEN GERENTESENIOR IS NOT NULL THEN GERENTESENIOR
                        WHEN GERENTEPLENO IS NOT NULL THEN GERENTEPLENO
                        WHEN GERENTE IS NOT NULL THEN GERENTE
                        ELSE GERENTE_EXECUTIVO 
                    END AS Gerente,
                    TipoHierarquia,
                    ROW_NUMBER() OVER (
                        PARTITION BY atributo
                        ORDER BY 
                            CASE 
                                WHEN GERENTESENIOR IS NOT NULL THEN 1
                                WHEN GERENTEPLENO IS NOT NULL THEN 2
                                WHEN GERENTE IS NOT NULL THEN 3
                                ELSE 4
                            END
                    ) AS rn,
                    operacaohominum
                FROM rlt.hmn (NOLOCK)
                WHERE data = CONVERT(date, GETDATE()-1)
                AND atributo IN (SELECT atributo FROM #base_hmn)
            )

            SELECT atributo, Gerente, TipoHierarquia, operacaohominum
            into #temph
            FROM gerentes_rank
            WHERE rn = 1
            AND Gerente IS NOT NULL;
                        
            select atributo, da_qualidade, da_planejamento, da_exop, periodo, ativo
            into #mg 
            from {SISTEMA_MATRIZ} (nolock)
            where da_exop <> 1
            and da_qualidade <> 3
            and da_planejamento <> 3
            and ativo in (0,1)

            select distinct th.atributo, gerente, tipohierarquia, operacaohominum, mg.da_qualidade, mg.da_planejamento, mg.da_exop, mg.periodo, mg.ativo
            from #temph th
            left join #mg mg
            on mg.atributo = th.atributo

            drop table #base_hmn
            drop table #temph
            drop table #mg
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2], "operacao": i[3], "da_qualidade": i[4], "da_planejamento": i[5], "da_exop": i[6], "periodo": i[7] or '', "ativo": i[8]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    #await set_cache(cache_key, resultados, CACHE_TTL_PADRAO)
    return resultados

async def get_atributos_apoio(area):
    cache_key = f"atributos_apoio:{area}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            if area == "Qualidade":
                cur.execute(f"""
                select distinct atributo, periodo from {SISTEMA_MATRIZ} (nolock) 
                where (gerente <> '' and tipo_matriz like 'OPERA%') 
                and da_qualidade = 0 and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
                """)
            elif area == "Planejamento":
                cur.execute(f"""
                select distinct atributo, periodo from {SISTEMA_MATRIZ} (nolock) 
                where (gerente <> '' and tipo_matriz like 'OPERA%') 
                and da_planejamento = 0 and periodo >= dateadd(d,1,eomonth(GETDATE(),-1))
                """)
            resultados = [{"atributo": i[0], "periodo": i[1]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    #await set_cache(cache_key, resultados, CACHE_TTL_PADRAO)
    return resultados

async def get_matrizes_nao_cadastradas():
    cache_key = f"matrizes_nao_cadastradas"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                declare @indice int = ?
                set nocount on
                select distinct atributo, count(matricula) as atendentes
                into #atrb_valid_hmn
                from rlt.hmn (nolock) 
                where data = convert(date, getdate()-1)
                and tipohierarquia = 'operação' and nivelhierarquico = 'operacional'
                and funcaorm not like '%analista%'
                and situacaohominum in ('ativo', 'treinamento')
                group by atributo, funcaorm

                select distinct atributo 
                into #in_mg_not_in_hmn 
                from {SISTEMA_MATRIZ} (nolock)
                where periodo = dateadd(d, 1, eomonth(getdate(), @indice))
                except
                select atributo 
                from #atrb_valid_hmn
                where atendentes > 0

                select atributo
                into #final
                from #atrb_valid_hmn
                where atendentes > 0
                and atributo is not null
                except
                select distinct atributo
                from {SISTEMA_MATRIZ} mg (nolock)
                where mg.atributo not in (select inmg.atributo from #in_mg_not_in_hmn inmg)
                and mg.periodo = dateadd(d, 1, eomonth(getdate(), @indice))
                        
                select distinct atributo,
                CASE 
                    WHEN GERENTESENIOR IS NOT NULL THEN GERENTESENIOR
                    WHEN GERENTEPLENO IS NOT NULL THEN GERENTEPLENO
                    WHEN GERENTE IS NOT NULL THEN GERENTE
                    ELSE GERENTE_EXECUTIVO 
                END AS Gerente,
                gerente_executivo,
                diretoratendimento,
                ROW_NUMBER() OVER (
                                PARTITION BY atributo
                                ORDER BY 
                                    CASE 
                                        WHEN GERENTESENIOR IS NOT NULL THEN 1
                                        WHEN GERENTEPLENO IS NOT NULL THEN 2
                                        WHEN GERENTE IS NOT NULL THEN 3
                                        ELSE 4
                                    END
                            ) AS rn
                into #hmn
                from rlt.hmn (nolock) 
                where data = convert(date, getdate()-1)
                        
                select distinct h.atributo, h.gerente, h.gerente_executivo, h.diretoratendimento from #final f
                left join #hmn h on f.atributo = h.atributo
                and h.rn = 1

                drop table #atrb_valid_hmn
                drop table #in_mg_not_in_hmn
                drop table #final
                drop table #hmn
            """, (0,))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{"atributo": row[0], "gerente": row[1], "gerente_executivo": row[2], "diretor_atendimento": row[3]} for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_PADRAO)
    return registros

async def get_pendencias_apoio():
    cache_key = f"pendencias_apoio"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct atributo, submetido_por, gerente, da_qualidade, da_planejamento, periodo from {SISTEMA_MATRIZ} (nolock) 
                where periodo >= dateadd(d, 1, eomonth(getdate(), ?)) and (da_qualidade = 0 or da_planejamento = 0)
            """, (-1,))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{"atributo": row[0], "submetido_por": row[1], "gerente": row[2], "da_qualidade": row[3], "da_planejamento": row[4], "periodo": row[5]} for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_PADRAO)
    return registros

async def get_all_atributos_cadastro_apoio(area):
    cache_key = f"all_atributos_cadastro_apoio:{area}"
    cached = await get_from_cache(cache_key)
    area_breviada = None
    if area == "qualidade":
        area_abreviada = "QUALID"
    elif area == "planejamento":
        area_abreviada = "PLAN"
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            set nocount on

            select distinct atributo, case when GERENTE is not null then GERENTE
            when GERENTEPLENO is not null then GERENTEPLENO
            when GERENTESENIOR is not null then GERENTESENIOR
            else GERENTE_EXECUTIVO end as Gerente 
            into #at from [robbyson].[rlt].[hmn] (nolock) where data = convert(date, getdate()-1) 
            and tipohierarquia = 'ADMINISTRAÇÃO' and nivelhierarquico = 'OPERACIONAL'
            and SituacaoHominum in ('ativo', 'treinamento')
            and atributo is not null
            and atributo like '%{area_abreviada}%'

            select * from #at where Gerente is NOT NULL

            drop table #at
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": "ADMINISTRAÇÃO"} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, CACHE_TTL_PADRAO)
    return resultados

async def get_num_atendentes(atributo):
    cache_key = f"num_atendentes:{atributo}"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            select count(matricula) as matriculas from [robbyson].[rlt].[hmn] (nolock) where data = convert(date, getdate()-1) 
            and atributo like '%{atributo}%'
            and tipohierarquia = 'OPERAÇÃO' and nivelhierarquico = 'OPERACIONAL'
            and SituacaoHominum in ('ativo', 'treinamento')
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados[0], CACHE_TTL_12H)
    return resultados[0]

async def get_atributos_matricula(matricula):
    cache_key = f"atributos_matricula:{matricula}"
    cached = await get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
            SET NOCOUNT ON

            select atributo, count(matricula) as matriculas into #qtd from [robbyson].[rlt].[hmn] (nolock) 
                        where data = convert(date, getdate()-1) 
                        and tipohierarquia = 'OPERAÇÃO' and nivelhierarquico = 'OPERACIONAL'
                        and SituacaoHominum in ('ativo', 'treinamento')
                        group by atributo
                        order by count(matricula) DESC

            select distinct hmn.atributo, case when GERENTESENIOR is not null then GERENTESENIOR
                        when GERENTEPLENO is not null then GERENTEPLENO
                        when GERENTE is not null then GERENTE
                        else GERENTE_EXECUTIVO end as Gerente, 
                        case when tipohierarquia = 'OPERAÇÃO' then 'OPERACIONAL' when tipohierarquia = 'ADMINISTRAÇÃO' then tipohierarquia else '' end as tipo from [robbyson].[rlt].[hmn] hmn (nolock) 
                        where (data = convert(date, getdate()-1)) and (hmn.atributo is not null) 
                        and (MatrGERENTE = {matricula} or MatrGERENTEPLENO = {matricula} or MatrGERENTESENIOR = {matricula} or MatrGERENTE_EXECUTIVO = {matricula})
                        and hmn.atributo in (select atributo from #qtd)
                        and tipohierarquia = 'OPERAÇÃO'

            drop table #qtd
            """)
            resultados = [{"atributo": i[0], "gerente": i[1], "tipo": i[2]} for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(cache_key, resultados, CACHE_TTL_12H)
    return resultados

async def get_excecoes_disponibilidade():
    cache_key = f"excecoes_disponibilidade"
    cached = await get_from_cache(cache_key)
    if cached:
        return cached
    resultados = None
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct atributo from robbyson.dbo.excecoes_disp_sistema_matriz (nolock)
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [row[0] for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_12H)
    return registros

async def get_funcao(matricula):
    cache_key = f"funcao:{matricula}"
    cached = await get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct funcaorm 
                from [robbyson].[rlt].[hmn] (nolock) 
                where data = convert(date, getdate()-1) 
                and matricula = '{matricula}'
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if resultados:
        await set_cache(cache_key, resultados[0], CACHE_TTL_12H)
        return resultados[0]
    return None

async def get_atributos_gerente(tipo, atributos, username):
    cache_key = f"all_atributos:{tipo}:{username}"
    cached = await get_from_cache(cache_key)
    resultados = None
    tipo_pesquisa_mg = None
    tipo_pesquisa_fef = None
    if tipo == 'm0_all':
        tipo_pesquisa_mg = -1
        tipo_pesquisa_fef = -1
    elif tipo == 'm+1_all':
        tipo_pesquisa_mg = 0
        tipo_pesquisa_fef = -1
    elif tipo == 'm1_all':
        tipo_pesquisa_mg = -2
        tipo_pesquisa_fef = -2
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                set nocount on
                select id_indicador, id_formato into #formatos from rby_indicador

                select fef.atributo, fef.id, fef.metasugerida, fef.resultado, fef.atingimento, f.id_formato 
                into #fef
                from Robbyson.dbo.factibilidadeEfaixas fef (nolock)
                left join #formatos f on f.id_indicador = fef.id
                where fef.data = DATEADD(DD, 1, EOMONTH(DATEADD(MM, ?, GETDATE())))

                select mg.atributo, id_nome_indicador, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.metasugerida AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.metasugerida, 'P') else CAST(ROUND(fef.metasugerida, 2) AS NVARCHAR(MAX)) end as metasugerida, 
                case when fef.id_formato = 4 then FORMAT(DATEADD(second, CAST(COALESCE(TRY_CAST(fef.resultado AS FLOAT), 0.0) AS BIGINT), '00:00:00'), 'HH:mm:ss') 
                when fef.id_formato = 3 then format(fef.resultado, 'P') else CAST(ROUND(fef.resultado, 2) AS NVARCHAR(MAX)) end as resultado, 
                format(fef.atingimento, 'P') as atingimento, mg.meta, moedas, tipo_indicador, 
                acumulado, esquema_acumulado, tipo_matriz, data_inicio, data_fim, periodo, escala, tipo_de_faturamento, descricao, ativo, 
                chamado, criterio, area, responsavel, gerente, possui_dmm, dmm, submetido_por, data_submetido_por, qualidade, da_qualidade, 
                data_da_qualidade, planejamento, da_planejamento, data_da_planejamento, exop, da_exop, data_da_exop
                from {SISTEMA_MATRIZ} mg (nolock)
                left join #fef fef on fef.id = LTRIM(RTRIM(LEFT(id_nome_indicador, CHARINDEX('-', id_nome_indicador) - 1)))
                and fef.atributo = mg.atributo
                WHERE mg.atributo IN ({atributos})
                and tipo_matriz like 'OPERA%'
                AND periodo = dateadd(d,1,eomonth(GETDATE(),?))
                AND ativo in (0, 1)
                order by atributo

                drop table #formatos
                drop table #fef

            """,(tipo_pesquisa_fef, tipo_pesquisa_mg))
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "atributo": row[0], "id_nome_indicador": row[1], "meta_sugerida": row[2] or '', "resultado": row[3] or '', "atingimento": row[4] or '', "meta": row[5], "moedas": row[6], "tipo_indicador": row[7] or '', "acumulado": row[8] or '', "esquema_acumulado": row[9] or '',
        "tipo_matriz": row[10] or '', "data_inicio": row[11], "data_fim": row[12], "periodo": row[13], "escala": row[14] or '', "tipo_de_faturamento": row[15] or '', "descricao": row[16] or '', "ativo": row[17], "chamado": row[18] or '',
        "criterio": row[19] or '', "area": row[20] or '', "responsavel": row[21] or '', "gerente": row[22] or '', "possui_dmm": row[23] or '', "dmm": row[24] or '',
        "submetido_por": row[25], "data_submetido_por": row[26], "qualidade": row[27], "da_qualidade": row[28], "data_da_qualidade": row[29],
        "planejamento": row[30], "da_planejamento": row[31], "data_da_planejamento": row[32], "exop": row[33], "da_exop": row[34], "data_da_exop": row[35], "id": str(uuid.uuid4())
    } for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_PADRAO)
    return registros

async def get_gerentes():
    cache_key = f"gerentes"
    cached = await get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                set nocount on
                select distinct gerente
                into #gerentes
                from {HOMINUM} where data = convert(date, getdate()-1) 
                and gerente is not null
                and operacaohominum is not null
                and atributo is not null
                union all
                select distinct GERENTEPLENO
                from {HOMINUM} where data = convert(date, getdate()-1) 
                and GERENTEPLENO is not null
                and operacaohominum is not null
                and atributo is not null
                union all
                select distinct gerentesenior
                from {HOMINUM} where data = convert(date, getdate()-1) 
                and gerentesenior is not null
                and operacaohominum is not null
                and atributo is not null
                union all
                select distinct gerente_executivo
                from {HOMINUM} where data = convert(date, getdate()-1) 
                and gerente_executivo is not null
                and operacaohominum is not null
                and atributo is not null

                select gerente from #gerentes order by gerente

                drop table #gerentes
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "gerente": row[0]
    } for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_12H)
    return registros

async def get_operacoes():
    cache_key = f"operacoes"
    cached = await get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct operacaohominum from {HOMINUM} where data = convert(date, getdate()-1) and operacaohominum is not null order by operacaohominum
            """)
            resultados = cur.fetchall()
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    registros = [{
        "operacao": row[0]
    } for row in resultados]
    await set_cache(cache_key, registros, CACHE_TTL_12H)
    return registros

async def get_nome(matricula):
    cache_key = f"nome:{matricula}"
    cached = await get_from_cache(cache_key)
    resultados = None
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select nome from rlt.hmn (nolock) where matricula = {matricula} and data = CONVERT(date, getdate()-1)
            """)
            resultados = [i[0] for i in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    if resultados:
        await set_cache(cache_key, resultados[0], CACHE_TTL_12H)
        return resultados[0]
    return None


async def get_names():
    cached = await get_from_cache(f"all_names")
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select distinct matricula, nome
                from rlt.hmn (nolock) where data = convert(date, getdate()-1)
                and ((NivelHierarquico like '%gerente%') or (NivelHierarquico like '%superintendente%'))
                and matricula is not null
            """)
            resultados = {row[0]: row[1] for row in cur.fetchall()}
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(f"all_names", resultados, CACHE_TTL_12H)
    return resultados
    
async def get_all_alterations():
    cached = await get_from_cache(f"all_alterations")
    if cached:
        return cached
    loop = asyncio.get_event_loop()
    def _sync_db_call():
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                select * from auditoria_sistema_matriz (nolock) where data_alterado_por >= dateadd(d, 1, eomonth(getdate(), -1)) order by data_alterado_por desc
            """)
            resultados = [{'atributo': row[0], 'id_nome_indicador': row[1], 'meta_antiga': row[2], 'nova_meta': row[3], 'alterado_por': row[4], 'resultado_m0': row[5], 'gerente': row[6], 'periodo': row[7], 'moeda_antiga': row[8], 'nova_moeda': row[9], 'dmm_antigo': row[10], 'novo_dmm': row[11], 'data_alterado_por': row[12], 'superintendente': row[13]} for row in cur.fetchall()]
            cur.close()
            return resultados
    resultados = await loop.run_in_executor(None, _sync_db_call)
    await set_cache(f"all_alterations", resultados, CACHE_TTL_PADRAO)
    return resultados
    