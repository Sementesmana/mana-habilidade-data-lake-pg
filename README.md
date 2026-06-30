# mana-habilidade-data-lake-pg

> **Cache durável JSONB no Postgres** pra esconder fontes lentas (SE/SA/APIs externas) atrás de uma camada de leitura rápida. Padrão **lake-first com fallback ao-vivo** + `advisory_lock` pra concorrência. Habilidade canônica da [Maná Builder](https://github.com/Sementesmana/template-habilidade-mana), sub-categoria **dados**.

## Por que existe

Todo painel Maná que depende de fonte lenta (SOAP SE, REST de terceiros, agregação cara) tinha o mesmo problema: no carregamento do painel, somar latências de N chamadas. O ganho real, medido no `agente-pedidos` em 2026-06-30: leitura via lake **204ms** vs leitura ao vivo **~2s** = **~10x mais rápido**.

A mesma necessidade apareceu em todo painel Maná. Esta habilidade encapsula o padrão num pacote reusável.

## Instalação

Distribuição por **git tag** (padrão Maná Builder):

```bash
pip install "git+https://github.com/Sementesmana/mana-habilidade-data-lake-pg.git@v0.1.0"
```

Dependência única: `psycopg2-binary>=2.9.10` (wheel Python 3.13).

## Uso típico

### Setup — 1× por agente

```python
import os
from mana_habilidade_data_lake_pg import DataLake

lake = DataLake(
    db_url=os.environ["DATABASE_URL"],
    schema="agente_pedidos",         # cada agente tem seu schema
    advisory_lock_id=778900,         # único por agente — evita colisão
)
lake.init_schema()                    # idempotente
```

### Caso 1 — Ingestão (cron + botão)

```python
lake.upsert("workflows", get_workflows_from_se())     # função pesada
lake.upsert("totais", get_totais_financeiro())
lake.upsert("atividades", get_atvsc())
```

### Caso 2 — Leitura **lake-first com fallback ao-vivo** (núcleo do valor)

```python
def listar_workflows():
    return lake.read_or_compute(
        chave="workflows",
        compute_fn=get_workflows_from_se,   # roda só se lake vazio
        max_age_hours=24,                   # opcional: re-computa se velho
    )
```

Comportamento:
- Se lake tem dado fresco → serve do lake (rápido)
- Se lake vazio ou velho → chama `compute_fn`, gravar de volta no lake
- Se `compute_fn` falha → serve o que tem no lake (mesmo velho) — **pipeline não morre**

### Caso 3 — Leitura simples

```python
dados, atualizado_em = lake.read("workflows")
if dados is None:
    print("lake vazio")
```

### Caso 4 — Status pro painel ("Atualizado em N min atrás")

```python
info = lake.status()
# [{"chave": "workflows", "atualizado_em": datetime, "tamanho_bytes": 12345}, ...]
```

### Caso 5 — Cron concorrente (Gunicorn N workers)

```python
# Cada worker tenta. Só 1 ingere de fato (os outros pulam imediatamente).
with lake.lock() as got:
    if not got:
        return  # outro worker está ingerindo
    lake.upsert("workflows", get_workflows_from_se())
    lake.upsert("totais", get_totais_financeiro())
```

## API pública

| Símbolo | Descrição |
|---|---|
| `DataLake(db_url, schema, advisory_lock_id, table_name="data_lake")` | Construtor |
| `.init_schema()` | Cria schema + tabela (idempotente) |
| `.upsert(chave, dados)` → `bool` | INSERT ON CONFLICT DO UPDATE; True se gravou |
| `.read(chave)` → `(dados, atualizado_em)` | `(None, None)` se chave não existe |
| `.read_or_compute(chave, compute_fn, max_age_hours=None)` → `Any` | Lake-first com fallback ao-vivo |
| `.delete(chave)` → `bool` | Remove chave; True se removeu |
| `.status()` → `list[dict]` | Lista chaves + timestamps + tamanho |
| `.try_lock()` / `.unlock()` → `bool` | Advisory lock manual (não-bloqueante) |
| `.lock()` → context manager | `with lake.lock() as got: ...` (yield True/False) |

## Schema padrão (criado por `init_schema()`)

```sql
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
    chave         TEXT PRIMARY KEY,
    dados         JSONB NOT NULL,
    atualizado_em TIMESTAMPTZ DEFAULT NOW()
);
```

Tabela única por agente, várias chaves lógicas (`workflows`, `totais`, `detalhe:<id>`, etc).

## Decisões canônicas

| Item | Valor | Por quê |
|---|---|---|
| Schema | Dedicado por agente | Isolamento; cada dono não pisa no outro |
| Tipo dos dados | `JSONB` | Flexível; índices GIN no futuro se precisar query interna |
| Concorrência | `pg_advisory_lock(id)` | 1 ingestão por vez mesmo com N workers Gunicorn |
| Fallback | Lake vazio/expirado → `compute_fn` | Pipeline nunca quebra; degradação graciosa |
| Frescor | `max_age_hours` opcional na leitura | Configurável por chave |
| Status | `lake.status()` → list de dicts | Pro painel mostrar "Atualizado em" |

## Comportamento defensivo (padrão Akita)

| Cenário | Comportamento |
|---|---|
| `db_url` vazio | `ValueError` no construtor (fail-fast) |
| `schema` ou `table_name` inválido (SQL injection) | `ValueError` |
| Erro de DB em `read` | `(None, None)` + log warning |
| Erro de DB em `upsert` | `False` + log warning |
| `compute_fn` levanta no `read_or_compute` | Serve dado velho do lake (não levanta) |
| `try_lock` / `unlock` / `status` falham | Retorno seguro (False / []) + log |

## Stack de dados Maná

Esta habilidade compõe um par com **`mana-habilidade-se-dataset-reader`**:

```
SE (Conjunto)  →  se-dataset-reader  →  data-lake-pg  →  agente/painel
   (L1 SE)        (L2 ingestão REST)   (L2 cache PG)    (L3 leitura)
```

Materializa o ADR [`mana-data-gateway-se-governa-banco-mana-serve`](https://github.com/Sementesmana/mana-vault) (2026-06-23).

## LGPD

Esta habilidade **transporta** dados como JSONB — não inspeciona conteúdo. Pode armazenar PII (se o consumidor ingerir). O **consumidor** decide se pseudonimiza antes de mandar pro LLM (use [`mana-habilidade-pseudonimizar-pii`](https://github.com/Sementesmana/mana-habilidade-pseudonimizar-pii)).

## Estado

**v0.1.0** (2026-06-30) — primeira release.

- ✅ Testes pytest com cobertura
- ✅ Lake-first com fallback ao-vivo
- ✅ Advisory lock + context manager
- ✅ Validação SQL injection (schema/table_name)
- ⏳ **`alpha`** — pendente migração do `agente-pedidos` (1º consumidor) pra cumprir gate beta

**Roadmap pro gate:**
1. Migrar `agente-pedidos` (remover `_lake_init`/`_lake_get`/`_lake_put` inline → usar `DataLake`) → `beta`
2. Segundo consumidor (agente-gestor-comercial ou agente-tms) → `producao`

## Dono

Xayer (@xayer-mana, Sementes Maná LTDA). Mudanças via PR (semver: PATCH=fix, MINOR=compatível, MAJOR=breaking + ADR).
