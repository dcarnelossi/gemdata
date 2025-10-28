import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

from modules.api_conection import make_request
from modules.dbpgconn import WriteJsonToPostgres
from modules.helpers import increment_one_day

# -----------------------------------------------------------------------------
# Config de logging básica (ajuste se já configurar em outro lugar)
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# -----------------------------------------------------------------------------
# Variáveis globais (preenchidas via set_globals)
# -----------------------------------------------------------------------------
api_conection_info = None
data_conection_info = None
coorp_conection_info = None


# -----------------------------------------------------------------------------
# Funções utilitárias da API VTEX OMS
# -----------------------------------------------------------------------------
def get_order_by_id(order_id: str):
    """
    Busca um pedido completo pelo ID no VTEX OMS.
    """
    return make_request(
        api_conection_info["Domain"],
        "GET",
        f"api/oms/pvt/orders/{order_id}",
        params=None,
        headers=api_conection_info["headers"],
    )


def get_orders_list_pages(query_params: dict):
    """
    Chama a listagem do VTEX OMS com os parâmetros de paginação/filtro.
    NÃO modifica 'query_params' in-place.
    """
    try:
        return make_request(
            api_conection_info["Domain"],
            "GET",
            "api/oms/pvt/orders",
            params=query_params,
            headers=api_conection_info["headers"],
        )
    except Exception as e:
        logging.error(f"Failed to retrieve orders list pages: {e}")
        raise


# -----------------------------------------------------------------------------
# Escrita no banco
# -----------------------------------------------------------------------------
def write_orders_to_db(order_id: str):
    """
    Baixa o JSON completo da order e upserta na tabela.
    """
    try:
        order_json = get_order_by_id(order_id)
    except Exception as e:
        logging.error(f"Erro ao buscar order {order_id}: {e}")
        raise

    try:
        writer = WriteJsonToPostgres(
            data_conection_info, order_json, "realtime_vtex_orders", "orderid"
        )
        writer.upsert_data2()
        logging.info("Upsert OK para order ID: %s", order_id)
    except Exception as e:
        logging.error(f"Erro ao gravar order {order_id} - {e}")
        raise


def process_orders(order_ids):
    """
    Recebe uma LISTA de IDs, ex.: ["1572170578694-01", "1572160578682-01", ...]
    Realiza o fetch individual + upsert em paralelo.
    """
    if not order_ids:
        logging.warning("Nenhum order_id recebido para processar.")
        return False

    try:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_order = {
                executor.submit(write_orders_to_db, order_id): order_id
                for order_id in order_ids
            }
            for future in concurrent.futures.as_completed(future_to_order):
                order_id = future_to_order[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Order {order_id} gerou exceção: {e}")
                    raise  # propaga para sinalizar falha geral
        return True
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")
        raise


# -----------------------------------------------------------------------------
# Paginação SOMENTE na listagem (orders_list)
# -----------------------------------------------------------------------------
def process_page(query_params: dict):
    """
    Pagina SOMENTE a listagem de pedidos (orders_list), retornando todos os orderIds
    de acordo com os filtros/intervalo informados em query_params.
    """
    try:
        per_page = int(query_params.get("per_page", 100))
        page = 1
        all_order_ids = []

        while True:
            params = dict(query_params)
            params["page"] = page
            params["per_page"] = per_page

            logging.info(f"Processing list page {page} with params: {params}")
            tentativa = 0
            resp = None

            # tenta até 5 vezes obter uma resposta válida (com leve backoff)
            while (resp is None or "list" not in resp) and tentativa < 5:
                try:
                    resp = get_orders_list_pages(params)
                except Exception as e:
                    logging.warning(
                        f"Tentativa {tentativa + 1}/5 falhou (page={page}): {e}"
                    )
                finally:
                    if resp is None or "list" not in resp:
                        time.sleep(2 + tentativa)  # backoff incremental
                        tentativa += 1

            if not resp or "list" not in resp:
                logging.error(f"No orders found for params (page={page}): {params}")
                break  # encerra paginação desse intervalo

            orders_list = resp.get("list") or []
            order_ids = [o.get("orderId") for o in orders_list if o.get("orderId")]
            all_order_ids.extend(order_ids)

            paging = resp.get("paging") or {}
            current_page = int(paging.get("currentPage", page))
            total_pages = int(paging.get("pages", page))

            logging.info(
                f"Collected {len(order_ids)} orderIds on page {current_page}/{total_pages}"
            )

            # fim da paginação?
            if current_page >= total_pages:
                break

            page += 1
            # opcional: pequeno sleep para respeitar rate limits
            time.sleep(0.2)

        return all_order_ids

    except Exception as e:
        logging.exception("Falha ao paginar orders_list")
        raise


# -----------------------------------------------------------------------------
# Varredura por dia (usa paginação só na listagem), depois upsert individual
# -----------------------------------------------------------------------------
def process_orders_lists(start_date, end_date):
    """
    Varre por dia, pagina SOMENTE a listagem (orders_list) para coletar todos os orderIds
    do dia e, em seguida, busca cada pedido individualmente e faz o upsert no banco.
    """
    try:
        data_inicial, data_final = validate_and_convert_dates(start_date, end_date)

        while data_inicial <= data_final:
            start_dia, end_dia = increment_one_day(data_inicial)

            qs = {
                "per_page": 100,  # limite típico do OMS
                "f_creationDate": f"creationDate:[{start_dia} TO {end_dia}]",
                # adicione outros filtros se precisar (status, canal, etc.)
            }

            logging.info(f"Processing orders from {start_dia} to {end_dia}.")

            # pagina SÓ a lista (orders_list) e junta todos os IDs do dia
            order_ids = process_page(qs)
            if not order_ids:
                logging.info(f"Nenhum pedido no intervalo {start_dia} a {end_dia}.")
            else:
                logging.info(
                    f"{len(order_ids)} pedidos encontrados no dia; iniciando processamento individual."
                )
                # processa as orders uma a uma e insere no banco (com pool)
                process_orders(order_ids)

            data_inicial += timedelta(days=1)

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing orders: {e}")
        raise  # falha a task em caso de erro


# -----------------------------------------------------------------------------
# Datas
# -----------------------------------------------------------------------------
def validate_and_convert_dates(start_date, end_date):
    """
    Converte strings 'YYYY-MM-DD' para datetime; se já forem datetime, só retorna.
    """
    try:
        if not isinstance(start_date, datetime):
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not isinstance(end_date, datetime):
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        return start_date, end_date
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        raise


# -----------------------------------------------------------------------------
# Entrada de dependências globais (mantido como no seu código)
# -----------------------------------------------------------------------------
def set_globals(api_info, data_conection, coorp_conection, **kwargs):
    """
    Define conexões globais e dispara o processamento por intervalo.
    kwargs deve conter: start_date, end_date (YYYY-MM-DD ou datetime)
    """
    global api_conection_info, data_conection_info, coorp_conection_info
    api_conection_info = api_info
    data_conection_info = data_conection
    coorp_conection_info = coorp_conection



    # Dispara o processamento com o intervalo informado
    process_orders_lists(kwargs["start_date"], kwargs["end_date"])

