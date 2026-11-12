import pytest
from prefect.testing.utilities import prefect_test_harness

from orquestra_pipeline import dbt_fluxo


@pytest.fixture(scope="module", autouse=True)
def prefect_local():
    with prefect_test_harness():
        yield


def test_dbt_passou(tmp_path, monkeypatch):
    chamadas = []
    monkeypatch.setattr(dbt_fluxo, "_construir", lambda csv, db: chamadas.append((csv, db)) or True)
    r = dbt_fluxo.pipeline_dbt(str(tmp_path / "a.csv"), str(tmp_path / "d.duckdb"))
    assert r == {"dbt_ok": True}
    assert chamadas[0][0] == tmp_path / "a.csv"


def test_dbt_falha_derruba_o_fluxo_e_tenta_de_novo(tmp_path, monkeypatch):
    chamadas = []
    monkeypatch.setattr(dbt_fluxo, "_construir", lambda csv, db: chamadas.append(1) or False)
    with pytest.raises(dbt_fluxo.DbtFalhou):
        dbt_fluxo.pipeline_dbt(str(tmp_path / "a.csv"), str(tmp_path / "d.duckdb"))
    assert len(chamadas) == 2  # 1 tentativa + 1 retry


def test_servir_agenda_com_cron(monkeypatch):
    recebido = {}
    monkeypatch.setattr(dbt_fluxo.pipeline_dbt, "serve", lambda **kw: recebido.update(kw))
    dbt_fluxo.servir("x.csv", "y.duckdb", cron="*/5 * * * *")
    assert recebido["cron"] == "*/5 * * * *"
    assert recebido["parameters"] == {"csv_path": "x.csv", "db": "y.duckdb"}


def test_sem_pipeline_consumidor_da_erro_claro(tmp_path, monkeypatch):
    import builtins
    real = builtins.__import__

    def falso(nome, *a, **k):
        if nome == "pipeline_consumidor":
            raise ImportError("sem módulo")
        return real(nome, *a, **k)

    monkeypatch.setattr(builtins, "__import__", falso)
    with pytest.raises(ImportError, match="pip install -e"):
        dbt_fluxo._construir(tmp_path / "a.csv", tmp_path / "d.duckdb")


def test_cli_dbt_ok_falha_e_agendar(tmp_path, monkeypatch, capsys):
    from orquestra_pipeline import cli
    monkeypatch.setattr(dbt_fluxo, "_construir", lambda csv, db: True)
    assert cli.main(["dbt", "--csv", "a.csv", "--db", str(tmp_path / "d.duckdb")]) == 0
    assert '"dbt_ok": true' in capsys.readouterr().out
    monkeypatch.setattr(dbt_fluxo, "_construir", lambda csv, db: False)
    assert cli.main(["dbt", "--csv", "a.csv", "--db", str(tmp_path / "d.duckdb")]) == 1
    visto = {}
    monkeypatch.setattr(dbt_fluxo, "servir", lambda csv, db, cron: visto.update(cron=cron))
    assert cli.main(["dbt", "--csv", "a.csv", "--agendar", "0 6 * * *"]) == 0
    assert visto["cron"] == "0 6 * * *"
