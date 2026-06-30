"""
Core — DataLake no Postgres (cache durável JSONB com fallback ao-vivo).

Extraído (e generalizado) de agente-pedidos/app.py (ADR 2026-06-30):
funções _lake_init, _lake_get, _lake_put, _ingerir_data_lake, _lake_cron_job.
Origem prática: data lake do agente-pedidos em produção 2026-06-30
(ganho ~10x: leitura lake 204ms vs live ~2s).

Padrão lake-first com fallback ao-vivo + advisory_lock pra concorrência.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

log = logging.getLogger("mana-habilidade-data-lake-pg")

TABLE_NAME_DEFAULT = "data_lake"


class DataLake:
    """Cache durável JSONB no Postgres.

    Schema padrão (criado por init_schema, idempotente):

        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            chave         TEXT PRIMARY KEY,
            dados         JSONB NOT NULL,
            atualizado_em TIMESTAMPTZ DEFAULT NOW()
        );

    Args:
        db_url: connection string Postgres (formato libpq).
        schema: schema dedicado por agente (ex: "agente_pedidos", "agente_tms").
                Isolamento — cada dono não pisa no outro.
        advisory_lock_id: int único por agente pra pg_try_advisory_lock.
                Evita 2 workers ingerirem em paralelo.
        table_name: nome da tabela (default "data_lake").

    LGPD:
        Esta habilidade TRANSPORTA dados como JSONB sem inspecionar conteúdo.
        Pode armazenar PII. Consumidor que envia pra LLM pseudonimiza no momento
        do uso, não na ingestão.
    """

    def __init__(
        self,
        db_url: str,
        schema: str,
        advisory_lock_id: int,
        table_name: str = TABLE_NAME_DEFAULT,
    ) -> None:
        if not db_url:
            raise ValueError("db_url não pode ser vazio")
        if not schema:
            raise ValueError("schema não pode ser vazio")
        if not isinstance(advisory_lock_id, int):
            raise ValueError("advisory_lock_id deve ser int")
        if not table_name or not table_name.replace("_", "").isalnum():
            raise ValueError("table_name deve ser identificador SQL válido")
        if not schema.replace("_", "").isalnum():
            raise ValueError("schema deve ser identificador SQL válido")

        self.db_url = db_url
        self.schema = schema
        self.advisory_lock_id = advisory_lock_id
        self.table_name = table_name
        self._qualified = f'"{schema}"."{table_name}"'  # quoted pra segurança

    # ── Conexão ──────────────────────────────────────────────────────

    def _connect(self) -> Any:
        """Abre conexão Postgres. Sempre fecha com `with conn` no caller."""
        return psycopg2.connect(self.db_url)

    # ── Schema ──────────────────────────────────────────────────────

    def init_schema(self) -> None:
        """Cria schema + tabela idempotente. Chamar 1× no boot do agente."""
        ddl_schema = f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"'
        ddl_table = (
            f"CREATE TABLE IF NOT EXISTS {self._qualified} ("
            "chave TEXT PRIMARY KEY, "
            "dados JSONB NOT NULL, "
            "atualizado_em TIMESTAMPTZ DEFAULT NOW())"
        )
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(ddl_schema)
            cur.execute(ddl_table)
        log.info("[LAKE %s] schema/tabela prontos", self.schema)

    # ── Leitura ──────────────────────────────────────────────────────

    def read(self, chave: str) -> tuple[Any, datetime | None]:
        """Lê uma chave do lake.

        Returns:
            (dados, atualizado_em) ou (None, None) se chave não existe.
        """
        if not chave:
            return None, None
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"SELECT dados, atualizado_em FROM {self._qualified} WHERE chave=%s",
                    (chave,),
                )
                row = cur.fetchone()
            if row:
                return row[0], row[1]
        except Exception as e:  # noqa: BLE001 — fail-soft no read
            log.warning("[LAKE %s] read %s: %s", self.schema, chave, e)
        return None, None

    def read_or_compute(
        self,
        chave: str,
        compute_fn: Callable[[], Any],
        max_age_hours: float | None = None,
    ) -> Any:
        """Lê do lake; se vazio OU mais velho que max_age_hours, computa ao vivo.

        Padrão **lake-first com fallback ao-vivo**. Quando o compute_fn levanta,
        retorna o que tem no lake (mesmo velho) — pipeline não morre.

        Args:
            chave: identificador lógico ('workflows', 'totais', etc).
            compute_fn: função sem args que computa o dado ao vivo.
            max_age_hours: se setado e o lake estiver mais velho, recomputa.

        Returns:
            Dados (do lake ou do compute_fn).
        """
        dados, atualizado_em = self.read(chave)

        # Snapshot ainda fresco — serve do lake
        if dados is not None and self._is_fresh(atualizado_em, max_age_hours):
            return dados

        # Sem dado ou velho — tenta computar ao vivo
        try:
            novo = compute_fn()
            if novo is not None:
                try:
                    self.upsert(chave, novo)
                except Exception as e:  # noqa: BLE001
                    log.warning("[LAKE %s] upsert pós-compute %s: %s", self.schema, chave, e)
            return novo
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] compute_fn %s falhou: %s — servindo lake (mesmo velho)", self.schema, chave, e)
            return dados  # melhor servir velho que nada

    @staticmethod
    def _is_fresh(atualizado_em: datetime | None, max_age_hours: float | None) -> bool:
        """True se atualizado_em é recente o suficiente (ou max_age_hours não setado)."""
        if max_age_hours is None:
            return atualizado_em is not None
        if atualizado_em is None:
            return False
        agora = datetime.now(timezone.utc)
        if atualizado_em.tzinfo is None:
            atualizado_em = atualizado_em.replace(tzinfo=timezone.utc)
        return (agora - atualizado_em) < timedelta(hours=max_age_hours)

    # ── Escrita ──────────────────────────────────────────────────────

    def upsert(self, chave: str, dados: Any) -> bool:
        """Insere ou atualiza uma chave. Atualiza atualizado_em pra now().

        Args:
            chave: identificador lógico.
            dados: qualquer JSON-serializable (dict, list, número, string).

        Returns:
            True se gravou, False se falhou (fail-soft).
        """
        if not chave:
            return False
        try:
            payload = json.dumps(dados, ensure_ascii=False, default=str)
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {self._qualified} (chave, dados, atualizado_em) "
                    "VALUES (%s, %s::jsonb, NOW()) "
                    "ON CONFLICT (chave) DO UPDATE SET "
                    "dados=EXCLUDED.dados, atualizado_em=NOW()",
                    (chave, payload),
                )
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] upsert %s: %s", self.schema, chave, e)
            return False

    def delete(self, chave: str) -> bool:
        """Remove uma chave. True se removeu, False se não existia ou falhou."""
        if not chave:
            return False
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self._qualified} WHERE chave=%s", (chave,))
                return cur.rowcount > 0
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] delete %s: %s", self.schema, chave, e)
            return False

    # ── Status (pro painel "Atualizado em") ──────────────────────────

    def status(self) -> list[dict[str, Any]]:
        """Lista chaves + timestamps + tamanho aproximado.

        Returns:
            [{"chave": str, "atualizado_em": datetime, "tamanho_bytes": int}, ...]
            Lista vazia se erro (fail-soft).
        """
        try:
            with self._connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"SELECT chave, atualizado_em, "
                    f"octet_length(dados::text) AS tamanho_bytes "
                    f"FROM {self._qualified} ORDER BY chave"
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] status: %s", self.schema, e)
            return []

    # ── Concorrência (advisory_lock) ─────────────────────────────────

    def try_lock(self) -> bool:
        """Tenta pegar o advisory_lock. True se conseguiu, False se já em uso.

        Usado pra cron rodar 1× mesmo com N workers Gunicorn. NÃO bloqueia
        (retorna False imediato se já travado por outro worker).
        """
        try:
            conn = self._connect()
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(%s)", (self.advisory_lock_id,))
                got = cur.fetchone()[0]
            if not got:
                conn.close()
            else:
                self._lock_conn = conn  # mantém aberta pro unlock
            return bool(got)
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] try_lock: %s", self.schema, e)
            return False

    def unlock(self) -> bool:
        """Libera o advisory_lock. True se liberou."""
        try:
            conn = getattr(self, "_lock_conn", None)
            if conn is None:
                return False
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (self.advisory_lock_id,))
                ok = cur.fetchone()[0]
            conn.close()
            self._lock_conn = None
            return bool(ok)
        except Exception as e:  # noqa: BLE001
            log.warning("[LAKE %s] unlock: %s", self.schema, e)
            return False

    @contextmanager
    def lock(self) -> Iterator[bool]:
        """Context manager pro advisory_lock.

        Yields:
            True se conseguiu o lock, False se já em uso por outro worker.

        Uso:
            >>> with lake.lock() as got:
            ...     if not got:
            ...         return  # outro worker está ingerindo
            ...     lake.upsert("workflows", get_workflows())
        """
        got = self.try_lock()
        try:
            yield got
        finally:
            if got:
                self.unlock()
