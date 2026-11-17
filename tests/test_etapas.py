import duckdb
import pytest

from orquestra_pipeline import etapas


@pytest.fixture
def db(tmp_path):
    csv = etapas.gerar_dados(tmp_path / "r.csv", linhas=200)
    banco = tmp_path / "t.duckdb"
    etapas.carregar_bronze(csv, banco)
    return banco


def test_gerar_dados_e_deterministico(tmp_path):
    a = etapas.gerar_dados(tmp_path / "a.csv", 50).read_text()
    b = etapas.gerar_dados(tmp_path / "b.csv", 50).read_text()
    assert a == b
    assert len(a.splitlines()) == 51


def test_bronze_conta_linhas(tmp_path):
    csv = etapas.gerar_dados(tmp_path / "r.csv", 120)
    assert etapas.carregar_bronze(csv, tmp_path / "t.duckdb") == 120


def test_prata_remove_duplicata_e_empresa_vazia(tmp_path):
    csv = tmp_path / "r.csv"
    csv.write_text("id,empresa,data,status,nota\n1,A,2026-01-01,respondida,5\n1,A,2026-01-01,respondida,5\n"
                   "2,,2026-01-02,respondida,3\n3, B ,2026-01-03,nao_respondida,\n")
    db = tmp_path / "t.duckdb"
    etapas.carregar_bronze(csv, db)
    assert etapas.transformar_prata(db) == 2
    con = duckdb.connect(str(db))
    assert {r[0] for r in con.execute("select empresa from prata").fetchall()} == {"A", "B"}


def test_ouro_e_qualidade_passam(db):
    etapas.transformar_prata(db)
    assert etapas.agregar_ouro(db) == len(etapas.EMPRESAS)
    assert all(etapas.checar_qualidade(db).values())


def test_qualidade_falha_com_nota_invalida(db):
    etapas.transformar_prata(db)
    etapas.agregar_ouro(db)
    con = duckdb.connect(str(db))
    con.execute("update prata set nota = 9 where id = (select min(id) from prata where nota is not null)")
    con.close()
    with pytest.raises(etapas.QualidadeError, match="nota_entre_1_e_5"):
        etapas.checar_qualidade(db)


def test_csv_so_com_cabecalho_passa_nas_checagens(tmp_path):
    csv = tmp_path / "vazio.csv"
    csv.write_text("id,empresa,data,status,nota\n")
    banco = tmp_path / "t.duckdb"
    assert etapas.carregar_bronze(csv, banco) == 0
    assert etapas.transformar_prata(banco) == 0
    assert etapas.agregar_ouro(banco) == 0
    assert all(etapas.checar_qualidade(banco).values())
