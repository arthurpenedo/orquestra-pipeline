import pytest

from orquestra_pipeline.cli import main


@pytest.mark.parametrize("comando", ["rodar", "dbt"])
def test_csv_inexistente_devolve_2_sem_traceback(comando, tmp_path, capsys):
    assert main([comando, "--csv", str(tmp_path / "nao.csv")]) == 2
    assert "CSV não encontrado" in capsys.readouterr().err


def test_rodar_linhas_zero_ou_negativo_devolve_2(tmp_path, capsys):
    for n in ("0", "-5"):
        assert main(["rodar", "--linhas", n, "--db", str(tmp_path / "o.duckdb")]) == 2
        assert "--linhas precisa ser 1 ou mais" in capsys.readouterr().err
    assert not (tmp_path / "o.duckdb").exists()
