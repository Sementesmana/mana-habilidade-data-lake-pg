"""
mana-habilidade-data-lake-pg — Cache durável JSONB no Postgres pra esconder
fontes lentas (SE/SA/APIs externas) atrás de uma camada de leitura rápida.

Habilidade canônica da Maná Builder, sub-categoria "dados" (par com
mana-habilidade-se-dataset-reader). Materializa ADR mana-data-gateway
(L2 cache local).

Padrão lake-first com fallback ao-vivo — se a fonte cai, pipeline não morre.
Lock por advisory_lock pra rodar 1× mesmo com N workers (Gunicorn).

USO TÍPICO

  Setup — 1x por agente
  -----------------------------------------------------------------
  >>> from mana_habilidade_data_lake_pg import DataLake
  >>> lake = DataLake(
  ...     db_url=os.environ["DATABASE_URL"],
  ...     schema="agente_pedidos",
  ...     advisory_lock_id=778900,  # único por agente
  ... )
  >>> lake.init_schema()  # idempotente

  Caso 1 — Upsert (ingestão)
  -----------------------------------------------------------------
  >>> lake.upsert("workflows", get_workflows_from_se())
  >>> lake.upsert("totais", get_totais_financeiro())

  Caso 2 — Leitura lake-first com fallback ao-vivo
  -----------------------------------------------------------------
  >>> def listar_workflows():
  ...     return lake.read_or_compute(
  ...         chave="workflows",
  ...         compute_fn=get_workflows_from_se,
  ...         max_age_hours=24,
  ...     )

  Caso 3 — Leitura simples
  -----------------------------------------------------------------
  >>> dados, atualizado_em = lake.read("workflows")
  >>> if dados is None:
  ...     print("lake vazio")

  Caso 4 — Status pro painel
  -----------------------------------------------------------------
  >>> info = lake.status()
  >>> # [{"chave": "workflows", "atualizado_em": ..., "tamanho_bytes": ...}, ...]

  Caso 5 — Lock pra cron concorrente (Gunicorn N workers)
  -----------------------------------------------------------------
  >>> if lake.try_lock():
  ...     try:
  ...         lake.upsert("workflows", ...)
  ...     finally:
  ...         lake.unlock()
"""

__version__ = "0.1.0"

from .core import DataLake

__all__ = ["DataLake"]
