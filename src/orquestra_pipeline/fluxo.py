"""O fluxo Prefect: cada etapa vira uma tarefa com tentativas e o fluxo registra o tempo de cada uma."""

import time
from pathlib import Path

from prefect import flow, get_run_logger, task

from . import etapas


def _medir(nome: str, funcao, *args):
    inicio = time.perf_counter()
    valor = funcao(*args)
    segundos = round(time.perf_counter() - inicio, 3)
    get_run_logger().info("%s: %s em %.3f s", nome, valor, segundos)
    return valor, segundos


@task(retries=2, retry_delay_seconds=1)
def t_bronze(csv_path, db):
    return _medir("bronze", etapas.carregar_bronze, csv_path, db)


@task
def t_prata(db):
    return _medir("prata", etapas.transformar_prata, db)


@task
def t_ouro(db):
    return _medir("ouro", etapas.agregar_ouro, db)


@task
def t_qualidade(db):
    return _medir("qualidade", etapas.checar_qualidade, db)


@flow(name="pipeline-consumidor-orquestrado")
def pipeline(csv_path: str, db: str = "orquestra.duckdb") -> dict:
    """bronze -> prata -> ouro -> qualidade. Se a qualidade falhar, o fluxo falha (e o Prefect marca)."""
    csv_path, db = Path(csv_path), Path(db)
    (n_bronze, s1) = t_bronze(csv_path, db)
    (n_prata, s2) = t_prata(db)
    (n_ouro, s3) = t_ouro(db)
    (checagens, s4) = t_qualidade(db)
    return {"bronze": n_bronze, "prata": n_prata, "ouro": n_ouro, "checagens": checagens,
            "segundos": {"bronze": s1, "prata": s2, "ouro": s3, "qualidade": s4}}
