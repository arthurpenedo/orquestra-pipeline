"""CLI: `python -m orquestra_pipeline rodar` gera dados sintéticos e roda o fluxo."""

import argparse
import json
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
    args = p.parse_args(argv)

    csv = Path(args.csv) if args.csv else etapas.gerar_dados(
        Path(tempfile.mkdtemp(prefix="orquestra-")) / "reclamacoes.csv", args.linhas)
    try:
        resultado = pipeline(str(csv), args.db)
    except etapas.QualidadeError as e:
        print(f"falhou na qualidade: {e}")
        return 1
    print(json.dumps(resultado, ensure_ascii=False, indent=2, default=str))
    return 0
