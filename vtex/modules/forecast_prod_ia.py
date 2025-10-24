# -*- coding: utf-8 -*-
"""
Forecast via OpenAI Responses API (compatível com openai==2.6.0)
- Limpa e normaliza datas localmente antes de chamar a API
- Força o forecast a começar em D+1 da última data real
- Corta qualquer linha de forecast anterior a D+1 (pós-processamento)
- NÃO salva CSVs: insere forecast no banco (inserir_forecast) e loga métricas/custo
"""

import os
import re
import csv
import json
import logging
from io import StringIO
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import subprocess
import sys


from modules.dbpgconn import WriteJsonToPostgres


def install(package):
    '''Funcao para instalar os pacotes via pip'''  
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar pacotes da lib sklearn se não estiver instalado
try:
   from openai import OpenAI
except ImportError:
    print("openai não está instalado. Instalando agora...")
    install("openai")

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# =========================================================
# VARIÁVEIS GLOBAIS (são setadas por set_globals)
# =========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None

MODEL_NAME = "gpt-4o"  # ou "gpt-4o-mini"
MODEL_PRICES = {
    "gpt-4o":      {"input_per_1k": 0.005,  "output_per_1k": 0.015},
    "gpt-4o-mini": {"input_per_1k": 0.00015,"output_per_1k": 0.0006},
}

SYSTEM_PROMPT = r"""
Você é um modelador de séries temporais. Regras obrigatórias:
1) Entrada: um CSV separado por pipe "|" com colunas:
   - data_dia (YYYY-MM-DD ou similar),
   - faturamento (numérico),
   - nm_feriado (string, opcional),
   - fl_feriado_ativo (boolean/0-1/true/false, opcional).
   A base pode NÃO estar diária; normalize para DIÁRIA.
2) Tratamento:
   - Parse de datas, ordenação, agregação por dia.
   - Reindexe diário (freq='D'). Preencha faltantes de faturamento com interpolação temporal
     e, se necessário, complete extremos com mediana robusta.
   - Padronize feriados: flag 0/1; nome string ('' se ausente).
   - Crie efeitos de pré-feriado: before_1d, before_2d, before_3d = 1 nos dias ANTERIORES ao feriado.
   - Modele efeitos de calendário (dia da semana e mês).
3) Modelo (NÃO USAR linha reta/naive):
   - Se disponível, use Prophet (seasonality weekly+yearly, modo multipliclicativo, changepoints razoáveis),
     com regressoras: is_holiday, before_1d, before_2d, before_3d e dummies/cíclicas de calendário.
   - Caso Prophet não esteja disponível, use um modelo não-linear tree-based (ex.: GradientBoostingRegressor)
     com lags (1,2,3,7,14,21,28), médias e desvios móveis (7,14,28), codificação cíclica de DOW e mês,
     e as variáveis de feriado/antecipação.
   - Faça validação holdout recente (ex.: últimos 14–28 dias, conforme tamanho) e reporte MAE, RMSE e MAPE%.
4) Forecast:
   - Horizonte = 3 meses à frente em base diária.
   - Intervalo de confiança ~80% (se tree-based, use banda empírica via desvio dos resíduos).
   - Se não houver calendário de feriados futuros, assuma 0 (sem feriados).
5) FORMATO DE SAÍDA:
   - Responda APENAS com um JSON válido (sem markdown), no formato:
     {
       "forecast_csv": "<CSV com colunas: data,forecast,lower80,upper80>",
       "info_csv": "<CSV com colunas: metric_or_config,value>"
     }
6) NÃO inclua cercas de código ``` nem comentários. Apenas o JSON puro.
"""

# =========================================================
# UTILS
# =========================================================
def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    prices = MODEL_PRICES.get(model, MODEL_PRICES["gpt-4o"])
    return (input_tokens/1000 * prices["input_per_1k"]) + (output_tokens/1000 * prices["output_per_1k"])

def extract_output_text(response) -> str:
    """Compatível com openai==2.6.0 (sem response_format)."""
    txt = getattr(response, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt
    out = getattr(response, "output", None)
    if out:
        chunks = []
        for item in out:
            content = getattr(item, "content", None)
            if content:
                for part in content:
                    t = None
                    if isinstance(part, dict):
                        if part.get("type") in ("output_text", "text"):
                            t = part.get("text")
                    else:
                        t = getattr(part, "text", None)
                    if isinstance(t, str):
                        chunks.append(t)
        if chunks:
            return "".join(chunks)
    choices = getattr(response, "choices", None)
    if choices and len(choices) > 0:
        msg = getattr(choices[0], "message", None) or {}
        content = getattr(msg, "content", None) or (isinstance(msg, dict) and msg.get("content"))
        if isinstance(content, str):
            return content
    return str(response)

def extract_json_block(text: str) -> str:
    """Remove cercas e extrai primeiro bloco JSON { ... }."""
    text = text.strip()
    text = re.sub(r"^```(json)?", "", text, flags=re.I).strip()
    text = re.sub(r"```$", "", text).strip()
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start:end+1].strip()
    return text

# =========================================================
# PRÉ-PROCESSAMENTO LOCAL
# =========================================================
def load_and_clean_input(df: pd.DataFrame):
    """
    Recebe df com colunas pipe-separated (já consultadas do banco),
    normaliza nomes, datas, soma por dia, etc. Retorna:
    - cleaned_csv  (texto pipe-separated limpo p/ enviar ao LLM)
    - last_real_date (Timestamp normalizada)
    - start_forecast (D+1)
    """
    logger.info("Executando limpeza/normalização local do dataframe de entrada…")

    cols = {c.strip().lower(): c for c in df.columns}
    date_col = cols.get("data_dia") or cols.get("data") or cols.get("dia") or list(df.columns)[0]
    rev_col  = cols.get("faturamento") or cols.get("receita") or cols.get("valor") or list(df.columns)[1]
    hol_name = cols.get("nm_feriado")
    hol_flag = cols.get("fl_feriado_ativo")

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col])
    df[rev_col] = pd.to_numeric(df[rev_col], errors="coerce")

    if hol_flag is None:
        df["fl_feriado_ativo"] = 0
        hol_flag = "fl_feriado_ativo"
    df[hol_flag] = (
        df[hol_flag]
        .astype(str).str.strip().str.lower()
        .map({"1":1,"true":1,"t":1,"sim":1,"yes":1,"y":1,"0":0,"false":0,"f":0,"nao":0,"não":0,"no":0})
        .fillna(0).astype(int)
    )
    if hol_name is None:
        df["nm_feriado"] = ""
        hol_name = "nm_feriado"
    else:
        df[hol_name] = df[hol_name].fillna("").astype(str)

    df = df.groupby(date_col, as_index=False).agg({
        rev_col: "sum",
        hol_flag: "max",
        hol_name: lambda s: next((x for x in s if isinstance(x, str) and x.strip() != ""), "")
    }).sort_values(date_col)

    last_real_date = pd.to_datetime(df[date_col].max()).normalize()
    start_forecast  = last_real_date + pd.Timedelta(days=1)

    df_ren = df.rename(columns={
        date_col: "data_dia",
        rev_col: "faturamento",
        hol_name: "nm_feriado",
        hol_flag: "fl_feriado_ativo",
    })[["data_dia","faturamento","nm_feriado","fl_feriado_ativo"]].copy()
    df_ren["data_dia"] = pd.to_datetime(df_ren["data_dia"]).dt.strftime("%Y-%m-%d")

    buf = StringIO()
    df_ren.to_csv(buf, sep="|", index=False, quoting=csv.QUOTE_MINIMAL, quotechar='"')
    cleaned_csv = buf.getvalue()

    logger.info("Limpeza/normalização concluída.")
    logger.info(f"Última data real detectada: {last_real_date.date()} | Início do forecast (D+1): {start_forecast.date()}")
    return cleaned_csv, last_real_date, start_forecast

# =========================================================
# DB / FORECAST
# =========================================================
def CriaDataFrameRealizado():
    """Consulta dados históricos realizados no SQL e devolve DataFrame."""
    logger.info("Executando consulta de realizados no banco…")
    try:
        query_realizado = f"""
            select 
              to_char(date_trunc('day', creationdate), 'YYYY-MM-DD')  as data_dia,
              round(cast(Sum(revenue) as numeric),0) as faturamento, 
              fff.nm_feriado,
              fff.fl_feriado_ativo
            from  orders_ia ord
            left join "5e164a4b-5e09-4f43-9d81-a3d22b09a01b".tb_forecast_feriado fff on
              to_char(date_trunc('day', dt_feriado), 'YYYY-MM-DD') = to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') 
            group by 
              to_char(date_trunc('day', creationdate), 'YYYY-MM-DD'),
              fff.nm_feriado,
              fff.fl_feriado_ativo
        """
        _, realizado = WriteJsonToPostgres(data_conection_info, query_realizado, "orders_ia").query()
        df_realizado = pd.DataFrame(realizado)
        logger.info("Consulta de realizados concluída.")
        return df_realizado
    except Exception as e:
        logger.error(f"Erro ao consultar realizados: {e}")
        raise

def inserir_forecast(future_df: pd.DataFrame):
    """Insere/atualiza previsões na tabela destino."""
    logger.info("Executando inserção de forecast no banco…")
    if future_df.empty:
        logger.warning("DataFrame de futuro vazio – nada a inserir.")
        return

    final_df = future_df[["creationdateforecast","predicted_revenue"]].copy()
    final_df["predicted_revenue"] = final_df["predicted_revenue"].round(2)

    hoje_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    hoje_str = hoje_dt.strftime("%Y-%m-%d")

    create_sql = """CREATE TABLE IF NOT EXISTS orders_ia_forecast (
        creationdateforecast TIMESTAMP PRIMARY KEY,
        predicted_revenue NUMERIC NOT NULL
    );"""
    WriteJsonToPostgres(data_conection_info, create_sql).execute_query_ddl()

    delete_sql = f"DELETE FROM orders_ia_forecast WHERE creationdateforecast >= '{hoje_str}';"
    WriteJsonToPostgres(data_conection_info, delete_sql).execute_query_ddl()

    count_sql = "SELECT COUNT(1) AS qtd FROM orders_ia_forecast;"
    _, res = WriteJsonToPostgres(data_conection_info, count_sql).query()
    primeira_execucao = res[0]["qtd"] == 0

    if primeira_execucao:
        logger.info("Primeira execução detectada – populando histórico do mês corrente…")
        df_realizado = CriaDataFrameRealizado()
        # ajuste se nomes divergirem
        df_realizado["data_dia"] = pd.to_datetime(df_realizado["data_dia"])
        hoje_dt = pd.to_datetime(hoje_dt)
        primeiro_dia_mes = hoje_dt.replace(day=1)
        filtro = (df_realizado["data_dia"] < hoje_dt) & (df_realizado["data_dia"] >= primeiro_dia_mes)
        df_hist = df_realizado.loc[filtro, ["data_dia", "faturamento"]].copy()
        df_hist.columns = ["creationdateforecast", "predicted_revenue"]
        df_hist["predicted_revenue"] = df_hist["predicted_revenue"].astype(float).round(2)
        WriteJsonToPostgres(data_conection_info, df_hist.to_dict("records"), "orders_ia_forecast", "creationdateforecast").insert_data_batch(df_hist.to_dict("records"))

    WriteJsonToPostgres(data_conection_info, final_df.to_dict("records"), "orders_ia_forecast", "creationdateforecast").insert_data_batch(final_df.to_dict("records"))
    logger.info("Inserção de forecast concluída.")

# =========================================================
# PIPELINE PRINCIPAL
# =========================================================
def executar():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("❌ OPENAI_API_KEY não encontrada no .env")

    client = OpenAI(api_key=api_key)

    # 1) REALIZADO
    logger.info("==> Executando etapa 1/6: carregar realizados do banco")
    realizado = CriaDataFrameRealizado()

    # 2) LIMPEZA / D+1
    logger.info("==> Executando etapa 2/6: limpeza/normalização e cálculo do D+1")
    cleaned_csv, last_real_date, start_forecast = load_and_clean_input(realizado)

    # 3) PROMPT COM REGRA D+1
    logger.info("==> Executando etapa 3/6: montar instruções para o modelo")
    instr = SYSTEM_PROMPT + f"""

7) IMPORTANTÍSSIMO:
   - Após parse/limpeza, a ÚLTIMA DATA REAL da série é {last_real_date.strftime('%Y-%m-%d')}.
   - O forecast DEVE começar exatamente em {start_forecast.strftime('%Y-%m-%d')} (D+1 da última data real).
   - NÃO inclua datas anteriores a {start_forecast.strftime('%Y-%m-%d')} no CSV de forecast.
"""

    # 4) CHAMADA API
    logger.info("==> Executando etapa 4/6: chamada à OpenAI (Responses API)")
    response = client.responses.create(
        model=MODEL_NAME,
        instructions=instr,
        input=[{
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": (
                    "Segue a BASE JÁ LIMPA no formato pipe-separated (use exatamente o conteúdo abaixo):\n"
                    "-----INICIO-ARQUIVO-----\n"
                    + cleaned_csv +
                    "\n-----FIM-ARQUIVO-----\n"
                    "Retorne APENAS o JSON no formato pedido (sem markdown)."
                ),
            }],
        }],
    )

    # 5) PARSE + PÓS-PROCESSAMENTO + INSERT
    logger.info("==> Executando etapa 5/6: parse da resposta e inserção no banco")
    raw_text = extract_output_text(response)
    json_str = extract_json_block(raw_text)
    try:
        data = json.loads(json_str)
    except Exception as e:
        logger.error("Falha ao decodificar JSON da resposta. Dump da resposta abaixo:")
        logger.error(raw_text)
        raise

    if "forecast_csv" not in data or "info_csv" not in data:
        raise ValueError("Resposta não contém chaves 'forecast_csv' e 'info_csv'.")

    # forecast_csv -> DataFrame
    fc = pd.read_csv(StringIO(data["forecast_csv"]))
    col_data = [c for c in fc.columns if c.strip().lower() == "data"]
    col_fore = [c for c in fc.columns if c.strip().lower() == "forecast"]
    if not col_data or not col_fore:
        raise ValueError("CSV de forecast deve conter colunas 'data' e 'forecast'.")
    col_data = col_data[0]
    col_fore = col_fore[0]

    # aplica corte D+1
    fc[col_data] = pd.to_datetime(fc[col_data], errors="coerce")
    fc = fc.dropna(subset=[col_data])
    fc = fc[fc[col_data] >= start_forecast]

    # adapta para o schema do inserir_forecast
    df_out = pd.DataFrame({
        "creationdateforecast": fc[col_data].dt.tz_localize(None),  # timestamp naive
        "predicted_revenue": pd.to_numeric(fc[col_fore], errors="coerce").astype(float)
    }).dropna(subset=["creationdateforecast", "predicted_revenue"])

    # INSERE NO BANCO
    inserir_forecast(df_out)

    # LOGA info_csv (métricas/config) sem salvar arquivo
    logger.info("==> Info do modelo / métricas (info_csv):")
    for line in data["info_csv"].splitlines():
        logger.info(line)

    # 6) TOKENS & CUSTO
    logger.info("==> Executando etapa 6/6: tokens e custo")
    usage = getattr(response, "usage", None)
    input_tokens  = getattr(usage, "input_tokens",  None)
    output_tokens = getattr(usage, "output_tokens", None)
    if input_tokens is None:  input_tokens  = getattr(usage, "prompt_tokens", 0) if usage else 0
    if output_tokens is None: output_tokens = getattr(usage, "completion_tokens", 0) if usage else 0
    total_tokens = (input_tokens or 0) + (output_tokens or 0)
    total_cost   = estimate_cost(MODEL_NAME, input_tokens or 0, output_tokens or 0)

    logger.info("--- USO E CUSTO ESTIMADO ---")
    logger.info(f"Modelo:        {MODEL_NAME}")
    logger.info(f"Input tokens:  {input_tokens or 0}")
    logger.info(f"Output tokens: {output_tokens or 0}")
    logger.info(f"Total tokens:  {total_tokens}")
    logger.info(f"Custo estimado: US${total_cost:.4f}")




def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    """Setter chamado externamente para informar conexões globais."""
    logger.info("Setando variáveis globais de conexão…")
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection

    executar()

    if not all([api_conection_info, data_conection_info, coorp_conection_info]):
        logger.error("Informações globais de conexão incompletas.")
        raise ValueError("All global connection information must be provided.")
    logger.info("Variáveis globais setadas com sucesso.")
