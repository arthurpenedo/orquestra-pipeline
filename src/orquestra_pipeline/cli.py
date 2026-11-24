"""CLI: `python -m orquestra_pipeline rodar` gera dados sintéticos e roda o fluxo."""

import argparse
import json
import sys
import tempfile
from pathlib import Path

from . import etapas
from .fluxo import pipeline


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="orquestra-pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("rodar", help="gera dados sintéticos e roda o fluxo")
    r.add_argument("--linhas", type=int, default=500)
    r.add_argument("--csv", help="CSV próprio (id, empresa, data, status, nota); sem isso gera dados sintéticos")
    r.add_argument("--db", default="orquestra.duckdb")
    d = sub.add_parser("dbt", help="roda o dbt build do pipeline-consumidor como fluxo Prefect")
    d.add_argument("--csv", required=True)
    d.add_argument("--db", default="consumidor.duckdb")
    d.add_argument("--agendar", metavar="CRON", help='em vez de rodar uma vez, serve o fluxo agendado (ex.: "0 6 * * *")')
    args = p.parse_args(argv)

    if args.cmd == "rodar" and args.linhas < 1:
        print("--linhas precisa ser 1 ou mais.", file=sys.stderr)
        return 2
    if args.csv and not Path(args.csv).is_file():
        print(f"CSV não encontrado: {args.csv}", file=sys.stderr)
        return 2

    if args.cmd == "dbt":
        from . import dbt_fluxo
        if args.agendar:
            dbt_fluxo.servir(args.csv, args.db, args.agendar)
            return 0
        try:
            print(json.dumps(dbt_fluxo.pipeline_dbt(args.csv, args.db)))
        except dbt_fluxo.DbtFalhou as e:
            print(f"falhou: {e}")
            return 1
        return 0

    csv = Path(args.csv) if args.csv else etapas.gerar_dados(
        Path(tempfile.mkdtemp(prefix="orquestra-")) / "reclamacoes.csv", args.linhas)
    try:
        resultado = pipeline(str(csv), args.db)
    except etapas.QualidadeError as e:
        print(f"falhou na qualidade: {e}")
        return 1
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
    return 0
