# -*- coding: utf-8 -*-
"""
Forecast via OpenAI Responses API (compatível com openai==2.6.0)
- Testa 4 modelos (GradientBoostingRegressor, XGBoost, Prophet, SARIMAX)
- Escolhe o melhor (menor MAPE)
- Insere o forecast normalmente (sem campo de modelo)
- Loga métricas e custo de execução
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
from airflow.models import Variable

from modules.dbpgconn import WriteJsonToPostgres


def install(package):
    """Instala pacotes ausentes via pip."""
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try:
    from openai import OpenAI
except ImportError:
    print("openai não está instalado. Instalando agora...")
    install("openai")

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# =========================================================
# VARIÁVEIS GLOBAIS
# =========================================================
api_conection_info = None
data_conection_info = None
coorp_conection_info = None
date_start_info = None

MODEL_NAME = "gpt-4o"
MODEL_PRICES = {
    "gpt-4o": {"input_per_1k": 0.005, "output_per_1k": 0.015},
    "gpt-4o-mini": {"input_per_1k": 0.00015, "output_per_1k": 0.0006},
}

# =========================================================
# PROMPT — compara 4 modelos e retorna o melhor
# =========================================================
SYSTEM_PROMPT = r"""
Você é um modelador de séries temporais. Regras obrigatórias:

1) Entrada:
   CSV separado por pipe "|" com colunas:
   - data_dia (YYYY-MM-DD)
   - faturamento (numérico)
   - nm_feriado (string, opcional)
   - fl_feriado_ativo (boolean, opcional)

2) Tratamento:
   - Parse de datas, ordenação e agregação diária.
   - Interpole faltantes e complete extremos com mediana.
   - Gere variáveis de calendário (dia da semana, mês, etc).
   - Gere indicadores de feriado e pré-feriado.

3) Modelagem:
   - Treine e compare os seguintes modelos:
       • GradientBoostingRegressor (sklearn)
       • XGBoost (xgboost.XGBRegressor)
       • Prophet (Facebook Prophet)
       • SARIMAX (statsmodels)
   - Use holdout (últimos 20–30 dias) para calcular MAE, RMSE, MAPE.
   - Escolha o melhor modelo (menor MAPE).
   - Produza o forecast com o melhor modelo.
   - Retorne as métricas e o nome do modelo vencedor no info_csv:
         metric_or_config,value
         best_model,Prophet
         MAE,123.45
         RMSE,234.56
         MAPE,3.21

4) Forecast:
   - Horizonte = 3 meses à frente, base diária.
   - Confiança 80% (ou banda empírica).
   - Se não houver feriados futuros, assuma 0.

5) Saída:
   - Responda APENAS com JSON puro:
     {
       "forecast_csv": "<CSV: data,forecast,lower80,upper80>",
       "info_csv": "<CSV: metric_or_config,value>"
     }
   - Não inclua ``` nem comentários.
"""

# =========================================================
# UTILS
# =========================================================
def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    prices = MODEL_PRICES.get(model, MODEL_PRICES["gpt-4o"])
    return (input_tokens / 1000 * prices["input_per_1k"]) + (output_tokens / 1000 * prices["output_per_1k"])

def extract_output_text(response) -> str:
    txt = getattr(response, "output_text", None)
    if isinstance(txt, str) and txt.strip():
        return txt
    out = getattr(response, "output", None)
    if out:
        parts = []
        for item in out:
            content = getattr(item, "content", None)
            if content:
                for c in content:
                    t = c.get("text") if isinstance(c, dict) else getattr(c, "text", None)
                    if t:
                        parts.append(t)
        return "".join(parts)
    return str(response)

def extract_json_block(text: str) -> str:
    text = text.strip()
    text = re.sub(r"^```(json)?", "", text, flags=re.I).strip()
    text = re.sub(r"```$", "", text).strip()
    start, end = text.find("{"), text.rfind("}")
    return text[start:end+1].strip() if start != -1 and end != -1 else text

# =========================================================
# PRÉ-PROCESSAMENTO LOCAL
# =========================================================
def load_and_clean_input(df: pd.DataFrame):
    """Normaliza e prepara o DataFrame antes de enviar ao modelo."""
    logger.info("Executando limpeza/normalização local do dataframe de entrada…")

    cols = {c.strip().lower(): c for c in df.columns}
    date_col = cols.get("data_dia") or list(df.columns)[0]
    rev_col  = cols.get("faturamento") or list(df.columns)[1]
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
        hol_name: lambda s: next((x for x in s if x.strip() != ""), "")
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
    df_ren.to_csv(buf, sep="|", index=False, quoting=csv.QUOTE_MINIMAL)
    cleaned_csv = buf.getvalue()

    logger.info(f"Última data real: {last_real_date.date()} | Início forecast: {start_forecast.date()}")
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
              to_char(date_trunc('day', creationdate), 'YYYY-MM-DD') as data_dia,
              round(sum(cast(revenue as numeric)),0) as faturamento,
              fff.nm_feriado,
              fff.fl_feriado_ativo
            from orders_ia ord
            left join public.tb_forecast_feriado fff on
              to_char(date_trunc('day', dt_feriado), 'YYYY-MM-DD') = to_char(date_trunc('day', creationdate), 'YYYY-MM-DD')
            where date_trunc('day', creationdate) < date_trunc('day', cast('{date_start_info}' as date))
            group by 1, fff.nm_feriado, fff.fl_feriado_ativo
        """
        _, realizado = WriteJsonToPostgres(data_conection_info, query_realizado, "orders_ia").query()
        df_realizado = pd.DataFrame(realizado)
        logger.info("Consulta concluída.")
        return df_realizado
    except Exception as e:
        logger.error(f"Erro ao consultar realizados: {e}")
        raise

def inserir_forecast(future_df: pd.DataFrame):
    """Insere previsões na tabela destino (sem nome de modelo)."""
    logger.info("Executando inserção de forecast no banco…")
    if future_df.empty:
        logger.warning("DataFrame de futuro vazio – nada a inserir.")
        return

    final_df = future_df.copy()
    final_df["predicted_revenue"] = final_df["predicted_revenue"].round(2)
    hoje_str = date_start_info.strftime("%Y-%m-%d")

    create_sql = """CREATE TABLE IF NOT EXISTS orders_ia_forecast (
        creationdateforecast TIMESTAMP PRIMARY KEY,
        predicted_revenue NUMERIC NOT NULL
    );"""
    WriteJsonToPostgres(data_conection_info, create_sql).execute_query_ddl()

    delete_sql = f"DELETE FROM orders_ia_forecast WHERE creationdateforecast >= '{hoje_str}';"
    WriteJsonToPostgres(data_conection_info, delete_sql).execute_query_ddl()

    WriteJsonToPostgres(
        data_conection_info, final_df.to_dict("records"), "orders_ia_forecast", "creationdateforecast"
    ).insert_data_batch(final_df.to_dict("records"))
    logger.info("Forecast inserido no banco com sucesso.")

# =========================================================
# PIPELINE PRINCIPAL
# =========================================================
def executar():
    load_dotenv()
    api_key = Variable.get("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)

    logger.info("==> Etapa 1: carregar realizados")
    realizado = CriaDataFrameRealizado()

    logger.info("==> Etapa 2: limpeza e D+1")
    cleaned_csv, last_real_date, start_forecast = load_and_clean_input(realizado)

    logger.info("==> Etapa 3: instruções")
    instr = SYSTEM_PROMPT + f"""

7) IMPORTANTÍSSIMO:
   - Última data real: {last_real_date.strftime('%Y-%m-%d')}
   - Forecast deve começar em {start_forecast.strftime('%Y-%m-%d')} (D+1)
   - Não inclua datas anteriores.
"""

    logger.info("==> Etapa 4: chamada OpenAI")
    response = client.responses.create(
        model=MODEL_NAME,
        instructions=instr,
        input=[{
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": (
                    "Segue a base limpa em formato pipe-separated:\n"
                    "-----INICIO-ARQUIVO-----\n"
                    + cleaned_csv +
                    "\n-----FIM-ARQUIVO-----\n"
                    "Retorne o JSON conforme o formato."
                ),
            }],
        }],
    )

    raw_text = extract_output_text(response)
    json_str = extract_json_block(raw_text)
    data = json.loads(json_str)

    fc = pd.read_csv(StringIO(data["forecast_csv"]))
    fc["data"] = pd.to_datetime(fc["data"], errors="coerce")
    fc = fc.dropna(subset=["data"])
    fc = fc[fc["data"] >= start_forecast]

    # identifica o melhor modelo (apenas para log)
    best_model = "Desconhecido"
    for line in data["info_csv"].splitlines():
        if line.lower().startswith("best_model"):
            best_model = line.split(",")[1].strip()
            break

    df_out = pd.DataFrame({
        "creationdateforecast": fc["data"].dt.tz_localize(None),
        "predicted_revenue": pd.to_numeric(fc["forecast"], errors="coerce").astype(float)
    }).dropna()

    inserir_forecast(df_out)

    logger.info("==> Info do modelo / métricas:")
    for line in data["info_csv"].splitlines():
        logger.info(line)
    logger.info(f"🏆 Melhor modelo escolhido: {best_model}")

    usage = getattr(response, "usage", None)
    input_tokens = getattr(usage, "input_tokens", 0)
    output_tokens = getattr(usage, "output_tokens", 0)
    total_cost = estimate_cost(MODEL_NAME, input_tokens, output_tokens)
    logger.info(f"💰 Custo estimado: US${total_cost:.4f}")

# =========================================================
# SET GLOBALS
# =========================================================
def set_globals(api_info, data_conection, coorp_conection, date_start, **kwargs):
    global api_conection_info, data_conection_info, coorp_conection_info, date_start_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection
    date_start_info = date_start
    executar()
