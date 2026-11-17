"""Etapas do pipeline (funções puras sobre DuckDB), sem nada do Prefect: dá para testar sozinhas."""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

import duckdb

EMPRESAS = ["Banco Alfa", "Telefonia Beta", "Varejo Gama", "Energia Delta", "Saude Epsilon"]
STATUS = ["respondida", "respondida", "respondida", "nao_respondida"]


class QualidadeError(Exception):
    """Uma checagem de qualidade de dados falhou."""


def gerar_dados(caminho: Path, linhas: int = 500, semente: int = 7) -> Path:
    """Reclamações sintéticas (nenhum dado real) para o pipeline rodar sem internet."""
    rng = random.Random(semente)
    inicio = date(2026, 1, 1)
    caminho = Path(caminho)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "empresa", "data", "status", "nota"])
        for i in range(linhas):
            nota = "" if rng.random() < 0.1 else rng.randint(1, 5)
            w.writerow([i, rng.choice(EMPRESAS), inicio + timedelta(days=rng.randint(0, 300)),
                        rng.choice(STATUS), nota])
    return caminho


def carregar_bronze(csv_path: Path, db: Path) -> int:
    """Bronze: o CSV do jeito que chegou. Devolve o número de linhas."""
    con = duckdb.connect(str(db))
    try:
        con.execute("create or replace table bronze as select * from read_csv_auto(?, header=true)",
                    [str(csv_path)])
        return con.execute("select count(*) from bronze").fetchone()[0]
    finally:
        con.close()


def transformar_prata(db: Path) -> int:
    """Prata: tipos certos, sem duplicata e sem empresa vazia."""
    con = duckdb.connect(str(db))
    try:
        con.execute("""
            create or replace table prata as
            select distinct id::int as id, trim(empresa) as empresa, data::date as data,
                   status, nota::int as nota
            from bronze where empresa is not null and trim(empresa) <> ''
        """)
        return con.execute("select count(*) from prata").fetchone()[0]
    finally:
        con.close()


def agregar_ouro(db: Path) -> int:
    """Ouro: ranking por empresa (reclamações, % respondidas, nota média)."""
    con = duckdb.connect(str(db))
    try:
        con.execute("""
            create or replace table ouro as
            select empresa, count(*) as reclamacoes,
                   round(100.0 * avg((status = 'respondida')::int), 1) as pct_respondidas,
                   round(avg(nota), 2) as nota_media
            from prata group by empresa order by reclamacoes desc
        """)
        return con.execute("select count(*) from ouro").fetchone()[0]
    finally:
        con.close()


def checar_qualidade(db: Path) -> dict[str, bool]:
    """Checagens do resultado. Levanta QualidadeError se alguma falhar."""
    con = duckdb.connect(str(db))
    try:
        um = lambda sql: con.execute(sql).fetchone()[0]
        resultado = {
            "prata_sem_id_duplicado": um("select count(*) - count(distinct id) from prata") == 0,
            "prata_nota_entre_1_e_5": um("select count(*) from prata where nota not between 1 and 5") == 0,
            "ouro_percentual_valido": um("select count(*) from ouro where pct_respondidas not between 0 and 100") == 0,
            "ouro_soma_confere": um("select coalesce(sum(reclamacoes), 0) from ouro") == um("select count(*) from prata"),
        }
    finally:
        con.close()
    falhas = [nome for nome, ok in resultado.items() if not ok]
    if falhas:
        raise QualidadeError(f"checagens que falharam: {', '.join(falhas)}")
    return resultado
