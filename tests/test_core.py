"""Testes da habilidade data-lake-pg. Mocka psycopg2.connect()."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from mana_habilidade_data_lake_pg import DataLake
from mana_habilidade_data_lake_pg.core import TABLE_NAME_DEFAULT


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────


def _mock_conn_cursor(fetchone_result=None, fetchall_result=None, rowcount=0):
    """Cria mocks de conn + cursor que funcionam com `with conn` + `with conn.cursor()`."""
    cur = MagicMock()
    cur.fetchone.return_value = fetchone_result
    cur.fetchall.return_value = fetchall_result or []
    cur.rowcount = rowcount
    # conn.cursor() retorna context manager
    cur_ctx = MagicMock()
    cur_ctx.__enter__ = lambda self: cur
    cur_ctx.__exit__ = lambda *a: None

    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cur_ctx)
    # `with conn` (psycopg2 commit/rollback semantics)
    conn.__enter__ = lambda self: conn
    conn.__exit__ = lambda *a: None
    return conn, cur


@pytest.fixture
def lake():
    return DataLake(
        db_url="postgresql://u:p@host:5432/db",
        schema="agente_teste",
        advisory_lock_id=12345,
    )


# ─────────────────────────────────────────────────────────────────────
# __init__ — validação
# ─────────────────────────────────────────────────────────────────────


def test_init_db_url_vazio_levanta():
    with pytest.raises(ValueError, match="db_url"):
        DataLake(db_url="", schema="s", advisory_lock_id=1)


def test_init_schema_vazio_levanta():
    with pytest.raises(ValueError, match="schema"):
        DataLake(db_url="x", schema="", advisory_lock_id=1)


def test_init_advisory_lock_nao_int_levanta():
    with pytest.raises(ValueError, match="advisory_lock_id"):
        DataLake(db_url="x", schema="s", advisory_lock_id="abc")  # type: ignore[arg-type]


def test_init_schema_invalido_levanta():
    with pytest.raises(ValueError, match="identificador SQL"):
        DataLake(db_url="x", schema="DROP TABLE", advisory_lock_id=1)


def test_init_table_name_invalido_levanta():
    with pytest.raises(ValueError, match="identificador SQL"):
        DataLake(db_url="x", schema="s", advisory_lock_id=1, table_name="DROP--")


def test_init_table_name_default():
    lake = DataLake(db_url="x", schema="s", advisory_lock_id=1)
    assert lake.table_name == TABLE_NAME_DEFAULT
    assert lake._qualified == '"s"."data_lake"'


def test_init_table_name_custom():
    lake = DataLake(db_url="x", schema="s", advisory_lock_id=1, table_name="meu_cache")
    assert lake._qualified == '"s"."meu_cache"'


# ─────────────────────────────────────────────────────────────────────
# init_schema
# ─────────────────────────────────────────────────────────────────────


def test_init_schema_executa_2_ddls(lake):
    conn, cur = _mock_conn_cursor()
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        lake.init_schema()
    # 1ª chamada = CREATE SCHEMA, 2ª = CREATE TABLE
    assert cur.execute.call_count == 2
    assert "CREATE SCHEMA" in cur.execute.call_args_list[0][0][0]
    assert "CREATE TABLE" in cur.execute.call_args_list[1][0][0]
    assert '"agente_teste"."data_lake"' in cur.execute.call_args_list[1][0][0]


# ─────────────────────────────────────────────────────────────────────
# read
# ─────────────────────────────────────────────────────────────────────


def test_read_chave_existe_retorna_tupla(lake):
    ts = datetime(2026, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
    conn, cur = _mock_conn_cursor(fetchone_result=({"x": 1}, ts))
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        dados, atualizado_em = lake.read("workflows")
    assert dados == {"x": 1}
    assert atualizado_em == ts


def test_read_chave_nao_existe_retorna_none_none(lake):
    conn, cur = _mock_conn_cursor(fetchone_result=None)
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        dados, atualizado_em = lake.read("inexistente")
    assert dados is None
    assert atualizado_em is None


def test_read_chave_vazia_retorna_none_none(lake):
    dados, atualizado_em = lake.read("")
    assert dados is None
    assert atualizado_em is None


def test_read_erro_db_retorna_none_none_fail_soft(lake):
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", side_effect=Exception("DB down")):
        dados, atualizado_em = lake.read("workflows")
    assert dados is None
    assert atualizado_em is None


# ─────────────────────────────────────────────────────────────────────
# upsert
# ─────────────────────────────────────────────────────────────────────


def test_upsert_executa_insert_on_conflict(lake):
    conn, cur = _mock_conn_cursor()
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        ok = lake.upsert("workflows", [{"id": 1}, {"id": 2}])
    assert ok is True
    sql = cur.execute.call_args[0][0]
    assert "INSERT INTO" in sql
    assert "ON CONFLICT" in sql
    # Payload JSON serializado
    chave, payload = cur.execute.call_args[0][1]
    assert chave == "workflows"
    assert '"id"' in payload  # serializou pra JSON


def test_upsert_chave_vazia_retorna_false_sem_chamar_db(lake):
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect") as mock_conn:
        ok = lake.upsert("", {"x": 1})
    assert ok is False
    mock_conn.assert_not_called()


def test_upsert_erro_db_retorna_false_fail_soft(lake):
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", side_effect=Exception("DB down")):
        ok = lake.upsert("workflows", {"x": 1})
    assert ok is False


def test_upsert_serializa_datetime_default(lake):
    """default=str do json.dumps converte datetime."""
    conn, cur = _mock_conn_cursor()
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        ok = lake.upsert("teste", {"quando": datetime(2026, 1, 1)})
    assert ok is True
    _, payload = cur.execute.call_args[0][1]
    assert "2026-01-01" in payload


# ─────────────────────────────────────────────────────────────────────
# read_or_compute (núcleo do valor — lake-first com fallback)
# ─────────────────────────────────────────────────────────────────────


def test_read_or_compute_lake_tem_dado_fresco_serve_do_lake(lake):
    """Lake tem dado e está fresco — não chama compute_fn."""
    ts_recente = datetime.now(timezone.utc) - timedelta(hours=1)
    conn, cur = _mock_conn_cursor(fetchone_result=({"do_lake": True}, ts_recente))
    compute = MagicMock(return_value={"computado": True})
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute, max_age_hours=24)
    assert result == {"do_lake": True}
    compute.assert_not_called()


def test_read_or_compute_lake_vazio_chama_compute_e_upsert(lake):
    """Lake vazio — chama compute e gravar de volta no lake."""
    conn, cur = _mock_conn_cursor(fetchone_result=None)
    compute = MagicMock(return_value=[1, 2, 3])
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute)
    assert result == [1, 2, 3]
    compute.assert_called_once()


def test_read_or_compute_lake_velho_recomputa(lake):
    """Lake mais velho que max_age_hours — recomputa."""
    ts_velho = datetime.now(timezone.utc) - timedelta(hours=48)
    conn, cur = _mock_conn_cursor(fetchone_result=({"velho": True}, ts_velho))
    compute = MagicMock(return_value={"novo": True})
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute, max_age_hours=24)
    assert result == {"novo": True}
    compute.assert_called_once()


def test_read_or_compute_compute_falha_serve_lake_velho(lake):
    """Se compute_fn levanta, serve o que tem no lake (mesmo velho) — pipeline não morre."""
    ts_velho = datetime.now(timezone.utc) - timedelta(hours=48)
    conn, cur = _mock_conn_cursor(fetchone_result=({"lake_velho": True}, ts_velho))

    def compute_que_falha():
        raise RuntimeError("upstream down")

    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute_que_falha, max_age_hours=1)
    assert result == {"lake_velho": True}


def test_read_or_compute_compute_falha_lake_vazio_retorna_none(lake):
    """Lake vazio + compute falha = None (vai do compute_fn que levanta)."""
    conn, cur = _mock_conn_cursor(fetchone_result=None)

    def compute_que_falha():
        raise RuntimeError("upstream down")

    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute_que_falha)
    # Lake vazio + compute falha = retorna None do lake
    assert result is None


def test_read_or_compute_max_age_none_serve_qualquer_idade(lake):
    """max_age_hours=None — qualquer dado do lake serve, mesmo antigo."""
    ts_antiquissimo = datetime(2020, 1, 1, tzinfo=timezone.utc)
    conn, cur = _mock_conn_cursor(fetchone_result=({"antigo": True}, ts_antiquissimo))
    compute = MagicMock(return_value={"novo": True})
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.read_or_compute("k", compute, max_age_hours=None)
    assert result == {"antigo": True}
    compute.assert_not_called()


# ─────────────────────────────────────────────────────────────────────
# delete
# ─────────────────────────────────────────────────────────────────────


def test_delete_chave_existente_retorna_true(lake):
    conn, cur = _mock_conn_cursor(rowcount=1)
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        assert lake.delete("k") is True


def test_delete_chave_inexistente_retorna_false(lake):
    conn, cur = _mock_conn_cursor(rowcount=0)
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        assert lake.delete("k") is False


def test_delete_chave_vazia_retorna_false(lake):
    assert lake.delete("") is False


# ─────────────────────────────────────────────────────────────────────
# status
# ─────────────────────────────────────────────────────────────────────


def test_status_retorna_lista_dicts(lake):
    rows = [
        {"chave": "workflows", "atualizado_em": datetime(2026, 6, 30, tzinfo=timezone.utc), "tamanho_bytes": 1024},
        {"chave": "totais", "atualizado_em": datetime(2026, 6, 30, tzinfo=timezone.utc), "tamanho_bytes": 512},
    ]
    conn, cur = _mock_conn_cursor(fetchall_result=rows)
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        result = lake.status()
    assert len(result) == 2
    assert result[0]["chave"] == "workflows"
    assert result[1]["tamanho_bytes"] == 512


def test_status_erro_retorna_lista_vazia_fail_soft(lake):
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", side_effect=Exception("DB down")):
        assert lake.status() == []


# ─────────────────────────────────────────────────────────────────────
# Advisory lock (try_lock / unlock / context manager)
# ─────────────────────────────────────────────────────────────────────


def test_try_lock_pega_lock_retorna_true(lake):
    conn, cur = _mock_conn_cursor(fetchone_result=(True,))
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        assert lake.try_lock() is True


def test_try_lock_ja_em_uso_retorna_false(lake):
    conn, cur = _mock_conn_cursor(fetchone_result=(False,))
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        assert lake.try_lock() is False


def test_try_lock_erro_retorna_false_fail_soft(lake):
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", side_effect=Exception("DB down")):
        assert lake.try_lock() is False


def test_unlock_sem_lock_anterior_retorna_false(lake):
    assert lake.unlock() is False


def test_lock_context_manager_yield_true_quando_pega(lake):
    """Com lock em mãos, yield True. Dentro do with, faz trabalho."""
    conn, cur = _mock_conn_cursor(fetchone_result=(True,))
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        with lake.lock() as got:
            assert got is True


def test_lock_context_manager_yield_false_quando_outro_worker_tem(lake):
    """Outro worker já pegou o lock — yield False."""
    conn, cur = _mock_conn_cursor(fetchone_result=(False,))
    with patch("mana_habilidade_data_lake_pg.core.psycopg2.connect", return_value=conn):
        with lake.lock() as got:
            assert got is False


# ─────────────────────────────────────────────────────────────────────
# Smoke
# ─────────────────────────────────────────────────────────────────────


def test_import_classe():
    assert DataLake is not None
