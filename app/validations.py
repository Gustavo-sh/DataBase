import copy
from datetime import datetime
import uuid
from fastapi.templating import Jinja2Templates
from fastapi import Response
from datetime import datetime, date
import calendar
from app.connections_db import get_resultados_indicadores_m3, get_all_atributos, get_excecoes_disponibilidade, get_indicadores
from app.utils import validar_horario

templates = Jinja2Templates(directory="app/templates")

async def validation_submit_table(registros, username, por_indicador, role):
    exceptions = await get_excecoes_disponibilidade()
    agora = datetime.now().strftime("%Y-%m-%d")
    moedas = 0
    presenca = []
    indicadores_duplicados = []
    dates_week_validation = []
    disp_in = False
    disp_mon = False
    nr_mon = False
    tl_mon = False
    is_exception_atribute = False
    atributo_trade = False
    is_replication = False
    atributo_inicial = registros[0]["atributo"]
    escala_inicial = registros[0]["escala"]
    resultados_indicadores_m3 = await get_resultados_indicadores_m3(atributo_inicial)
    indicadores_processados = set()
    operational_matrix = registros[0]["tipo_matriz"].lower() != "administração"
    for dic in registros:
        meta_val = dic.get("meta", "")
        nome_val = dic.get("id_nome_indicador", "").lower()
        id_indicador = int(dic["id_nome_indicador"].split(" - ")[0])

        is_exception_atribute = True if dic["atributo"] in exceptions else False
        try:
            moeda_val = int(dic.get("moedas", "0"))
        except ValueError:
            return "<p>Moeda deve ser um valor inteiro, valor informado: " + moeda_val + ", para o indicador: " + dic["id_nome_indicador"] + " e atributo: " + dic["atributo"] +".</p>"
        




        chave_indicador = (dic["atributo"], dic["id_nome_indicador"], moeda_val)
        if chave_indicador in indicadores_processados:
            moeda_val = 0
            dates_week_validation.append((dic["atributo"], dic["id_nome_indicador"]))
        else:
            indicadores_processados.add(chave_indicador)
        #validações para indicadores semanais, que entram na matriz duplicados com o mesmo valor de moedas
        #serve para que os valores de moedas depois do primeiro não contem

        if dic["escala"] != escala_inicial:
            return "<p>As escalas dos indicadores devem ser iguais, foi identificado um indicador com escala diferente, indicador: " + dic["id_nome_indicador"] + " e atributo: " + dic["atributo"] +".</p>"
        if dic["atributo"] == atributo_inicial:
            if not atributo_trade:
                moedas += moeda_val
        else:
            operacional_matrix = dic["tipo_matriz"].lower() != "administração"
            resultados_indicadores_m3 = await get_resultados_indicadores_m3(dic["atributo"])
            if id_indicador not in resultados_indicadores_m3 and operacional_matrix and role != "adm":
                return f"<p>Não é possível cadastrar o indicador {dic['id_nome_indicador']}, pois ele não tem resultados para os ultimos dois meses+mes atual para o atributo {dic['atributo']}.</p>"
            if moedas != 30 and moedas != 35:
                return "<p>A soma das moedas do atributo " + dic["atributo"] + " deve ser 30 ou 35, soma atual: " + str(moedas) + ".</p>"
            if moedas == 30 and dic["tipo_matriz"].lower() != "administração":
                input_presence(registros, username, agora, dic["atributo"], presenca)
            if moedas == 35 and not disp_mon:
                return "<p>Para somar 35 moedas, o indicador de disponibilidade deve estar monetizado! Atributo: " + dic["atributo"] + ".</p>"
            if is_exception_atribute and not disp_mon and moedas == 35:
                return "<p>Atributos de exceção que não monetizarem disponibilidade devem ter uma soma total de 30 moedas! Atributo: " + dic["atributo"] + ".</p>"
            atributo_inicial = dic["atributo"]
            atributo_trade = True
            is_replication = True
        #detecta a troca de atributo, uma vez que esta função é usada não só para submeter uma tabela, mas também para replicar registros para outros atributos





        
        if not is_replication:
            if moeda_val < 0: 
                return "<p>O valor de moedas deve ser positivo. O indicador " + dic["id_nome_indicador"] + " possui moedas negativas.</p>"
            if dic["tipo_indicador"] == "HORA":
                try:
                    if not validar_horario(str(meta_val)):
                        return "<p>O valor digitado em meta não foi um valor de hora no formato HH:MM:SS.</p>"
                except Exception as e:
                    return "<p>Digite um valor de hora no formato HH:MM:SS para o indicador " + dic["id_nome_indicador"] + ".</p>"
            else:
                try:
                    if dic["tipo_indicador"] == "INTEIRO":
                        if int(meta_val) <= 0:
                            return "<p>Meta deve ser um número positivo, valor informado: " + meta_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
                        meta_val = int(meta_val)
                        dic["meta"] = meta_val
                    else: 
                        if float(meta_val) <= 0:
                            return "<p>Meta deve ser um número positivo, valor informado: " + meta_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
                        meta_val = float(meta_val)
                        dic["meta"] = meta_val
                except ValueError:
                    return "<p>Meta deve ser um número válido, valor informado: " + meta_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
            #validações de meta e moedas





            chave_duplicada = (dic["atributo"], dic["id_nome_indicador"], dic["data_inicio"], dic["data_fim"])
            if chave_duplicada in indicadores_duplicados:
                return "<p>Indicador duplicado encontrado - Atributo: " + dic["atributo"] + ", Indicador: " + dic["id_nome_indicador"] + ", Período: " + dic["data_inicio"] + " a " + dic["data_fim"] + ".</p>"
            else:
                indicadores_duplicados.append((dic["atributo"], dic["id_nome_indicador"], dic["data_inicio"], dic["data_fim"]))
            #checagem de tentativa de registros duplicados




            if moeda_val > 0 and moeda_val < 3:
                return "<p>A monetização mínima é de 3 moedas. O indicador " + dic["id_nome_indicador"] + " possui menos de 3 moedas.</p>"
            if nome_val == r"6 - % absenteísmo" and (moeda_val != 0 or meta_val == "" or meta_val == 0):
                return "<p>Absenteísmo não pode ter moedas e deve ter uma meta diferente de zero, o atributo " + dic["atributo"] + " não atende esses requisitos</p>"
            if nome_val == r"901 - % disponibilidade":
                if meta_val != 94 or meta_val != 94.0:
                    return "<p>Disponibilidade deve ter 94 de meta, o atributo " + dic["atributo"] + " possui meta diferente de 94.</p>"
                if (moeda_val < 8) and not is_exception_atribute:
                    return "<p>Disponibilidade não pode ter menos que 8 moedas, o atributo " + dic["atributo"] + " possui menos que 8 moedas.</p>"
                disp_in = True
                if moeda_val > 0:
                    disp_mon = True
            if nome_val == "25 - pausa nr17":
                if moeda_val > 0:
                    nr_mon = True
            if nome_val == "15 - tempo logado":
                if moeda_val > 0:
                    tl_mon = True
            if (nome_val == "25 - pausa nr17" or nome_val == "15 - tempo logado") and (moeda_val != 0 or meta_val != "00:00:00") and not is_exception_atribute:
                return "<p>O valor de moeda deve ser 0 e o valor de meta para Pausa NR17 e Tempo Logado deve ser 00:00:00, o atributo " + dic["atributo"] + " não atende esses requisitos.</p>"
            #regras de negocio parte 1
        




        if id_indicador not in resultados_indicadores_m3 and operational_matrix and role != "adm":
            return f"<p>Não é possível cadastrar o indicador {dic['id_nome_indicador']}, pois ele não tem resultados para os ultimos dois meses+mes atual para o atributo {dic['atributo']}.</p>"
        #regra de negocio que precisa iterar todos os indicadores por todos os atributos e verificar se eles tem resultados tanto na replicação quanto no submit
        #fim do for



    if len(dates_week_validation) > 0:
        validation_dates_indicator_for_week(registros.copy(), dates_week_validation, por_indicador)
    if not disp_in and operational_matrix:
        return "<p>Disponibilidade é um indicador obrigatório, por favor adicione-o com 8 ou mais moedas e 94 de meta para o atributo.</p>"
    if disp_in and disp_mon and (nr_mon or tl_mon):
        return "<p>Não é permitido monetizar Pausa NR17 ou Tempo Logado quando Disponibilidade está monetizada.</p>"
    if moedas != 30 and moedas != 35:
        return "<p>A soma de moedas deve ser igual a 30 ou 35.</p>"
    if moedas == 35 and not disp_mon:
        return "<p>Para somar 35 moedas, o indicador de disponibilidade deve estar monetizado!</p>"
    if is_exception_atribute and not disp_mon and moedas == 35:
        return "<p>Atributos de exceção que não monetizarem disponibilidade devem ter uma soma total de 30 moedas!</p>"
    if moedas == 30 and registros[0]["tipo_matriz"].lower() != "administração":
        input_presence(registros, username, agora, registros[0]["atributo"], None) #none como ultimo parametro faz com que presença seja imputada em registros
    for dic in presenca:
        registros.append(dic)
    #regras de negocio parte 2


    return registros

def validation_dates_indicator_for_week(registros, dates_week_validation, por_indicador):
    possible_combinations = [
    # semanas individuais
    '17',    # 01–07
    '814',   # 08–14
    '1521',  # 15–21
    '2228',  # 22–28
    '2931',  # 29–31
    '2930',  # 29–30

    # junções válidas
    '114',   # 01–14
    '121',   # 01–21
    '128',   # 01–28
    '130',   # 01–30
    '131',   # 01–31

    '821',   # 08–21
    '828',   # 08–28
    '830',   # 08–30
    '831',   # 08–31

    '1528',  # 15–28
    '1530',  # 15–30
    '1531',  # 15–31

    '2231',  # 22–31
    '2230',  # 22–30
    ]
    for atributo, indicador in dates_week_validation:
        periodos = por_indicador.get((atributo, indicador), [])

        for dic in periodos:
            dia_inicio = dic["_dt_inicio"].day
            dia_fim = dic["_dt_fim"].day
            combination = str(dia_inicio) + str(dia_fim)
            if combination not in possible_combinations:
                raise ValueError(f"O indicador {indicador} do atributo {atributo} possui um período inválido: {dic['data_inicio']} a {dic['data_fim']}. Os períodos válidos são semanais.")
            

def input_presence(registros, username, agora, atributo, presenca):
    appendar = registros
    if isinstance(presenca, list):
        appendar = presenca
    try:
        appendar.append({'atributo': f'{atributo}', 'id_nome_indicador': '48 - PRESENÇA', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'DECIMAL', 'acumulado': 'NÃO', 'esquema_acumulado': 'DIÁRIO',
                        'tipo_matriz': 'OPERACIONAL', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                        'tipo_de_faturamento': 'CONTROLE', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio': 'META AEC', 'area': 'PLANEJAMENTO', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                        'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                        'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
    except KeyError:
        appendar.append({'atributo': f'{atributo}', 'id_nome_indicador': '48 - PRESENÇA', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'DECIMAL', 'acumulado': 'NÃO', 'esquema_acumulado': 'DIÁRIO',
                    'tipo_matriz': 'OPERACIONAL', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                    'tipo_de_faturamento': 'CONTROLE', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'META AEC', 'area': 'PLANEJAMENTO', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                    'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{username}', 'data_submetido_por': f'{agora}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                    'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})

async def validation_import_from_excel(registros, request):
    periodo = registros[0].get("periodo", "")
    atributos = await get_all_atributos()
    indicadores = [i.get("id") + " - " + i.get("text") for i in await get_indicadores()]
    for i, v in enumerate(registros): 
        if v.get("periodo", "") != periodo:
            return f"O período deve ser o mesmo para todos os registros, período encontrado diferente: {v.get('periodo', '')}, Linha {i+2}."
        if v["atributo"] not in atributos:
            return f"O atributo {v['atributo']} não é válido, por favor corrija o valor e tente novamente, Linha {i+2}."
        if v["id_nome_indicador"] not in indicadores:
            return f"O id_nome_indicador {v['id_nome_indicador']} não é válido, por favor corrija o valor e tente novamente. Linha {i+2}."
        if v["possui_dmm"] == "SIM":
            if len(v.get("dmm", "").split(",")) != 5 or (v.get("dmm", "") == "" or v.get("dmm", "") is None):
                return f"Indicador com DMM deve conter exatamente 5 dmms separados por vírgula O atributo {v['atributo']} possui {len(v.get('dmm', '').split(','))} dmms."
        elif v["possui_dmm"] == "NAO":
            if v.get("dmm", "") != "" and v.get("dmm", "") is not None:
                return f"Se 'Não' for colocado no campo 'possui_dmm' o campo 'dmm' deve ficar vazio, Linha {i+2}."
        else:
            return f"O campo 'possui_dmm' deve ser preenchido com 'SIM' ou 'NAO', valor encontrado: {v['possui_dmm']}, Linha {i+2}."
        if v["tipo_indicador"] not in ["HORA", "INTEIRO", "DECIMAL", "PERCENTUAL"]:
            return f"Tipo de indicador inválido, valor encontrado: {v['tipo_indicador']}, Linha {i+2}. Os tipos válidos são: HORA, INTEIRO, DECIMAL e PERCENTUAL."
        if v["acumulado"] == "SIM":
            if v["esquema_acumulado"] != "SEMANAL":
                return f"Esquema acumulado inválido para indicador acumulado, valor encontrado: {v['esquema_acumulado']}, Linha {i+2}. O esquema válido é: SEMANAL."
        elif v["acumulado"] == "NAO":
            if v["esquema_acumulado"] not in ["MENSAL", "DIARIO"]:
                return f"Esquema acumulado inválido para indicador não acumulado, valor encontrado: {v['esquema_acumulado']}, Linha {i+2}. O esquema válido é: DIARIO ou MENSAL."
        else:
            return f"O campo 'acumulado' deve ser preenchido com 'SIM' ou 'NAO', valor encontrado: {v['acumulado']}, Linha {i+2}."
        if v["tipo_matriz"] not in ["ADMINISTRAÇÃO", "OPERACIONAL"]:
            return f"Tipo de matriz inválido, valor encontrado: {v['tipo_matriz']}, Linha {i+2}. Os tipos válidos são: Administração e Operacional."
        if v["escala"] not in ["5X2", "6X1"]:
            return f"Escala invária, valor encontrado: {v['escala']}, Linha {i+2}. As escalas válidas são: 5X2 e 6X1."
        if v["tipo_de_faturamento"] not in ["ONUS", "ONUS E BONUS", "RECEITA", "RELACIONAMENTO", "CONTROLE"]:
            return f"Tipo de faturamento inválido, valor encontrado: {v['tipo_de_faturamento']}, Linha {i+2}. Os tipos válidos são: ONUS, ONUS E BONUS, RECEITA, RELACIONAMENTO e CONTROLE."
        if v["criterio"] not in ["META AEC", "META SMART", "META CLIENTE", "META FORECAST"]:
            return f"Criterio inválido, valor encontrado: {v['criterio']}, Linha {i+2}. Os critérios válidos são: META AEC, META SMART, META CLIENTE e META FORECAST."
    return None

async def validation_meta_moedas(registros, meta, moedas, role):
    tipo = registros["tipo_indicador"]
    if moedas != "" and moedas != None:
        if int(moedas) < 3 and int(moedas) > 0:
            return f"A monetização mínima é de 3 moedas. O indicador {registros['id_nome_indicador']} possui {moedas} moedas."
    
        if registros["id_nome_indicador"].lower() == r"901 - % disponibilidade" and int(moedas) < 8:
            return f"A monetização mínima do indicador 901 % Disponibilidade é de 8 moedas."
        try:
            int(moedas)
        except Exception:
            return f"Moedas deve ser um número inteiro! indicador: {registros['id_nome_indicador']}, moedas informada: {moedas}"
    if meta != "" and meta != None:
        if registros["id_nome_indicador"].lower() == r"901 - % disponibilidade" and int(meta) != 94:
            return f"Não é permitido alterar a meta do indicador 901 - % disponibilidade!"
        if tipo == "HORA":
            try:
                if len(meta.split(":")) != 3:
                    return f"Erro de meta para indicador tipo hora! indicador:{registros['id_nome_indicador']}, meta informada:{meta}. O modelo correto é HH:MM:SS"
            except Exception:
                return f"Erro de valor meta! indicador:{registros['id_nome_indicador']}, meta:{meta}"
        elif tipo == "INTEIRO":
            try:
                int(meta)
            except Exception:
                return f"Meta deve ser um valor inteiro! indicador:{registros['id_nome_indicador']}, meta informada:{meta}"
        elif tipo == "DECIMAL":
            try:
                float(meta)
            except Exception:
                return f"Meta deve ser um valor decimal! indicador:{registros['id_nome_indicador']}, meta informada:{meta}"
        elif tipo == "PERCENTUAL":
            try:
                float(meta)
            except Exception:
                return f"Meta deve ser um número válido! indicador:{registros['id_nome_indicador']}, meta informada:{meta}"
    
    return None

async def validation_dmm(dmm):
    try:
        qtd_dmm = len(dmm.split(","))
        if qtd_dmm != 5:
            return f"Selecione extamente 5 dmms! Dmms selecionados: {qtd_dmm}"
    except Exception as e:
        return f"Erro na validação de dmm: {e}"
    return None