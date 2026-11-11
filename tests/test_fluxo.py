import pytest
from prefect.testing.utilities import prefect_test_harness

from orquestra_pipeline import cli, etapas
from orquestra_pipeline.fluxo import pipeline


@pytest.fixture(scope="module", autouse=True)
def prefect_local():
    with prefect_test_harness():
        yield


def test_fluxo_completo(tmp_path):
    csv = etapas.gerar_dados(tmp_path / "r.csv", 300)
    r = pipeline(str(csv), str(tmp_path / "t.duckdb"))
    assert r["bronze"] == 300
    assert r["prata"] == 300
    assert r["ouro"] == len(etapas.EMPRESAS)
    assert all(r["checagens"].values())
    assert set(r["segundos"]) == {"bronze", "prata", "ouro", "qualidade"}


def test_fluxo_falha_com_csv_inexistente(tmp_path):
    with pytest.raises(Exception):  # noqa: B017  (o Prefect repassa o erro do DuckDB depois das tentativas)
        pipeline(str(tmp_path / "nao_existe.csv"), str(tmp_path / "t.duckdb"))


def test_cli_rodar(tmp_path, capsys):
    assert cli.main(["rodar", "--linhas", "100", "--db", str(tmp_path / "c.duckdb")]) == 0
    assert '"bronze": 100' in capsys.readouterr().out
