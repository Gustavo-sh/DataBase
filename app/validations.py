from datetime import datetime
import uuid
from fastapi.templating import Jinja2Templates
from fastapi import Response
from datetime import datetime, date
import calendar
from app.connections_db import get_resultados_indicadores_m3, get_all_atributos, get_excecoes_disponibilidade

templates = Jinja2Templates(directory="app/templates")

async def validation_submit_table(registros, username, por_indicador):
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
    resultados_indicadores_m3 = await get_resultados_indicadores_m3(atributo_inicial)
    indicadores_processados = set()
    operational_matrix = registros[0]["tipo_matriz"].lower() != "administração"
    for dic in registros:


        is_exception_atribute = True if dic["atributo"] in exceptions else False
        try:
            moeda_val = int(dic.get("moedas", "0"))
        except ValueError:
            return "<p>Moeda deve ser um valor inteiro, valor informado: " + moeda_val + ", para o indicador: " + dic["id_nome_indicador"] + ".</p>"
        meta_val = dic.get("meta", "")
        nome_val = dic.get("id_nome_indicador", "").lower()
        id_indicador = int(dic["id_nome_indicador"].split(" - ")[0])




        chave_indicador = (dic["atributo"], dic["id_nome_indicador"], moeda_val)
        if chave_indicador in indicadores_processados:
            moeda_val = 0
            dates_week_validation.append((dic["atributo"], dic["id_nome_indicador"]))
        else:
            indicadores_processados.add(chave_indicador)
        #validações para indicadores semanais, que entram na matriz duplicados com o mesmo valor de moedas
        #serve para que os valores de moedas depois do primeiro não contem


        if dic["atributo"] == atributo_inicial:
            if not atributo_trade:
                moedas += moeda_val
        else:
            resultados_indicadores_m3 = await get_resultados_indicadores_m3(dic["atributo"])
            if id_indicador not in resultados_indicadores_m3:
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
            if dic["tipo_indicador"] == "Hora":
                try:
                    if len(dic["meta"].split(':')) < 3:
                        return "<p>O valor digitado em meta não foi um valor de hora no formato HH:MM:SS.</p>"
                except Exception as e:
                    return "<p>Digite um valor de hora no formato HH:MM:SS para o indicador " + dic["id_nome_indicador"] + ".</p>"
            else:
                try:
                    if dic["tipo_indicador"] == "Inteiro":
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
                return "<p>Absenteísmo não pode ter moedas e deve ter uma meta diferente de zero.</p>"
            if nome_val == r"901 - % disponibilidade":
                if meta_val != 94 or meta_val != 94.0:
                    return "<p>Disponibilidade deve ter 94 de meta.</p>"
                if (moeda_val < 8) and not is_exception_atribute:
                    return "<p>Disponibilidade não pode ter menos que 8 moedas.</p>"
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
                return "<p>O valor de moeda deve ser 0 e o valor de meta para Pausa NR17 e Tempo Logado deve ser 00:00:00.</p>"
            #regras de negocio parte 1
        




        if id_indicador not in resultados_indicadores_m3 and operational_matrix:
            return f"<p>Não é possível cadastrar o indicador {dic['id_nome_indicador']}, pois ele não tem resultados para os ultimos dois meses+mes atual. Atributo: {dic['atributo']}.</p>"
        #regra de negocio que precisa iterar todos os indicadores por todos os atributos e verificar se eles tem resultados tanto na replicação quanto no submit
        #fim do for



    if len(dates_week_validation) > 0:
        validation_dates_indicator_for_week(registros.copy(), dates_week_validation, por_indicador)
    if not disp_in and operational_matrix:
        return "<p>Disponibilidade é um indicador obrigatório, por favor adicione-o com 8 ou mais moedas e 94 de meta.</p>"
    if disp_in and disp_mon and (nr_mon or tl_mon):
        return "<p>Não é permitido monetizar Pausa NR17 ou Tempo Logado quando Disponibilidade está monetizada.</p>"
    if moedas != 30 and moedas != 35:
        print(moedas)
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
        appendar.append({'atributo': f'{atributo}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                        'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                        'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': f'{registros[0]["chamado"]}', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                        'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{registros[0]["submetido_por"]}', 'data_submetido_por': f'{registros[0]["data_submetido_por"]}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                        'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})
    except KeyError:
        appendar.append({'atributo': f'{atributo}', 'id_nome_indicador': '48 - Presença', 'meta': '2', 'moedas': 5, 'tipo_indicador': 'Decimal', 'acumulado': 'Não', 'esquema_acumulado': 'Diário',
                    'tipo_matriz': 'Operacional', 'data_inicio': f'{registros[0]["data_inicio"]}', 'data_fim': f'{registros[0]["data_fim"]}', 'periodo': f'{registros[0]["periodo"]}', 'escala': f'{registros[0]["escala"]}',
                    'tipo_de_faturamento': 'Controle', 'descricao': f'{registros[0]["descricao"]}', 'ativo': 0, 'chamado': '', 'criterio': 'Meta AeC', 'area': 'Planejamento', 'responsavel': '', 'gerente': f'{registros[0]["gerente"]}', 
                    'possui_dmm': f'{registros[0]["possui_dmm"]}', 'dmm': f'{registros[0]["dmm"]}', 'submetido_por': f'{username}', 'data_submetido_por': f'{agora}', 'qualidade': '', 'da_qualidade': 3, 'data_da_qualidade': '', 
                    'planejamento': '', 'da_planejamento': 3, 'data_da_planejamento': '', 'exop': '', 'da_exop': 0, 'data_da_exop': '', 'justificativa': '', 'da_superintendente': '', 'id': uuid.uuid4()})

async def validation_import_from_excel(registros, request):
    registros_copia = registros
    retorno = []
    atributos = await get_all_atributos()
    for i in registros_copia: 
        if i["atributo"] not in atributos:
            i["descricao"] = "Erro de atributo,"
            retorno.append(i)
            continue
        if len(i["id_nome_indicador"].split(" - ")) != 2:
            i["descricao"] = "Erro de indicador,"
            retorno.append(i)
            continue
        if i["tipo_indicador"] == "Hora":
            try:
                if len(i["meta"].split(":")) != 3:
                    i["descricao"] = "Erro de meta para indicador tipo hora,"
                    retorno.append(i)
                    continue
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Inteiro":
            try:
                int(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de meta para indicador tipo hora,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Decimal":
            try:
                float(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        elif i["tipo_indicador"] == "Percentual":
            try:
                float(i["meta"])
            except ValueError:
                i["descricao"] = "Erro de valor meta,"
                retorno.append(i)
                continue
        try:
            int(i["moedas"])
        except ValueError:
            i["descricao"] = "Erro de valor moedas,"
            retorno.append(i)
            continue

    html_content = templates.TemplateResponse(
    "_pesquisa.html", 
    {"request": request, "registros": retorno} 
    )
    response = Response(content=html_content.body, media_type="text/html")
    response.headers["HX-Trigger"] = '{"mostrarSucesso": "xImportx: A validação encontrou erros, veja-os na primeira tabela abaixo!"}'
    if len(retorno) > 0:
        return response
    return None

async def validation_meta_moedas(registros, meta, moedas, role):
    tipo = registros["tipo_indicador"]
    # area = registros.get("area", "").lower()
    # if "qualidade" in role.lower() and (area != "qualidade" and area != ""):
    #     return f"xPesquisax: Usuário com perfil de Qualidade não pode alterar registros de outras áreas! indicador:{registros["id_nome_indicador"]}"
    # elif "planejamento" in role.lower() and (area != "planejamento" and area != ""):
    #     return f"xPesquisax: Usuário com perfil de Planejamento não pode alterar registros de outras áreas! indicador:{registros["id_nome_indicador"]}"
    if moedas != "" and moedas != None:
        if int(moedas) < 3 and int(moedas) > 0:
            return f"A monetização mínima é de 3 moedas. O indicador {registros['id_nome_indicador']} possui {moedas} moedas."
    if registros["id_nome_indicador"].lower() == r"901 - % disponibilidade" and int(meta) != 94:
        return f"Não é permitido alterar a meta do indicador 901 - % disponibilidade!"
    if tipo == "Hora":
        try:
            if len(meta.split(":")) != 3:
                return f"Erro de meta para indicador tipo hora! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}. O modelo correto é HH:MM:SS"
        except Exception:
            return f"Erro de valor meta! indicador:{registros["id_nome_indicador"]}, meta:{meta}"
    elif tipo == "Inteiro":
        try:
            int(meta)
        except Exception:
            return f"Meta deve ser um valor inteiro! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Decimal":
        try:
            float(meta)
        except Exception:
            return f"Meta deve ser um valor decimal! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    elif tipo == "Percentual":
        try:
            float(meta)
        except Exception:
            return f"Meta deve ser um número válido! indicador:{registros["id_nome_indicador"]}, meta informada:{meta}"
    try:
        if moedas != "" and moedas != None:
            int(moedas)
    except Exception:
        return f"Moedas deve ser um número inteiro! indicador: {registros["id_nome_indicador"]}, moedas informada: {moedas}"
    return None

async def validation_dmm(dmm):
    try:
        qtd_dmm = len(dmm.split(","))
        if qtd_dmm != 5:
            return f"Selecione extamente 5 dmms! Dmms selecionados: {qtd_dmm}"
    except Exception as e:
        return f"Erro na validação de dmm: {e}"
    return None

def validation_datas(data_inicio_bd, data_fim_bd, data_inicio_sbmit, data_fim_submit):
    data_original = datetime.strptime(data_inicio_sbmit, '%Y-%m-%d').date()
    ano = data_original.year
    mes = data_original.month
    _, ultimo_dia_do_mes = calendar.monthrange(ano, mes) 
    ultimo_dia_data = date(ano, mes, ultimo_dia_do_mes)
    ultimo_dia_str = ultimo_dia_data.strftime('%Y-%m-%d')
    if data_inicio_sbmit > data_inicio_bd and data_inicio_sbmit > data_fim_bd and data_inicio_sbmit <= data_fim_submit and data_inicio_sbmit <= ultimo_dia_str:
        if data_fim_submit > data_inicio_bd and data_fim_submit > data_fim_bd and data_fim_submit <= ultimo_dia_str:
            pass
        else:
            return True
    else:
        return True
