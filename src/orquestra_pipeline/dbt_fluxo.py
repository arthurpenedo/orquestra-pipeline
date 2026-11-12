"""Roda o `dbt build` do pipeline-consumidor como tarefa do Prefect e agenda o fluxo com cron."""

from pathlib import Path

from prefect import flow, get_run_logger, task


class DbtFalhou(RuntimeError):
    """O `dbt build` terminou com teste ou modelo falhando."""


def _construir(csv: Path, db: Path) -> bool:
    # import preguiçoso: o pipeline-consumidor é opcional (pip install -e ../pipeline-consumidor)
    try:
        from pipeline_consumidor import runner
    except ImportError as e:
        raise ImportError("instale o pipeline-consumidor para usar o dbt: pip install -e ../pipeline-consumidor") from e
    return runner.construir(csv, db)


@task(retries=1, retry_delay_seconds=1)
def t_dbt_build(csv: Path, db: Path) -> bool:
    ok = _construir(csv, db)
    get_run_logger().info("dbt build: %s", "passou" if ok else "FALHOU")
    if not ok:
        raise DbtFalhou("dbt build falhou (veja os testes de qualidade no log do dbt)")
    return ok


@flow(name="dbt-consumidor-orquestrado")
def pipeline_dbt(csv_path: str, db: str = "consumidor.duckdb") -> dict:
    """Roda o dbt build (bronze, prata, ouro e testes). Se algum teste do dbt falhar, o fluxo falha."""
    ok = t_dbt_build(Path(csv_path), Path(db))
    return {"dbt_ok": ok}


def servir(csv_path: str, db: str = "consumidor.duckdb", cron: str = "0 6 * * *") -> None:
    """Deixa o fluxo do dbt agendado (cron padrão: todo dia às 6h). Bloqueia até Ctrl+C."""
    pipeline_dbt.serve(name="dbt-diario", cron=cron, parameters={"csv_path": str(csv_path), "db": str(db)})
