import pytest

from orquestra_pipeline.cli import main


@pytest.mark.parametrize("comando", ["rodar", "dbt"])
def test_csv_inexistente_devolve_2_sem_traceback(comando, tmp_path, capsys):
    assert main([comando, "--csv", str(tmp_path / "nao.csv")]) == 2
    assert "CSV não encontrado" in capsys.readouterr().err
