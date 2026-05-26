# Airbnb dbt Project

Projeto dbt de modelagem analítica sobre dados do Airbnb, utilizando **Snowflake** como data warehouse. O objetivo é demonstrar boas práticas de engenharia de dados: arquitetura em camadas, contratos de dados, testes automatizados, rastreamento histórico com SCD Types e macros reutilizáveis.

---

## Índice

1. [Visão Geral da Arquitetura](#1-visão-geral-da-arquitetura)
2. [Fontes de Dados](#2-fontes-de-dados)
3. [Estrutura de Pastas](#3-estrutura-de-pastas)
4. [Camadas de Modelos](#4-camadas-de-modelos)
   - [src — Staging](#41-src--staging-ephemeral)
   - [dim — Dimensões](#42-dim--dimensões-table)
   - [fct — Fatos](#43-fct--fatos-incremental)
   - [mart — Consumo](#44-mart--consumo-table)
5. [SCD Types](#5-scd-types)
6. [Snapshots](#6-snapshots)
7. [Seeds](#7-seeds)
8. [Macros](#8-macros)
9. [Testes](#9-testes)
10. [Analyses](#10-analyses)
11. [Como Rodar](#11-como-rodar)

---

## 1. Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│  Snowflake — schema: RAW                                        │
│  raw_listings · raw_hosts · raw_reviews                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │  source()
          ┌────────────────▼────────────────┐
          │  src/  (ephemeral)              │
          │  src_listings                   │
          │  src_hosts                      │
          │  src_reviews                    │
          └──────┬─────────────┬────────────┘
                 │             │
    ┌────────────▼──┐   ┌──────▼─────────────────┐
    │  dim/ (table) │   │  fct/ (incremental)     │
    │  dim_listings │   │  fct_reviews            │
    │  dim_hosts    │   └──────┬──────────────────┘
    │  dim_listing  │          │
    │  _w_hosts     │   ┌──────▼──────────────────┐
    │               │   │  mart/ (table)           │
    │  SCD 1,2,3,4  │   │  mart_fullmoon_reviews   │
    └───────────────┘   └─────────────────────────┘
          │
    ┌─────▼──────────────────┐
    │  snapshots/            │
    │  scd_raw_listings      │
    │  scd_raw_hosts         │
    └────────────────────────┘
```

### Por que essa estrutura em camadas?

Cada camada tem uma responsabilidade única e bem definida, o que garante que qualquer mudança na fonte de dados afete apenas a camada `src`, protegendo os modelos de negócio abaixo dela.

| Camada | Responsabilidade | Materialização |
|---|---|---|
| `src` | Renomear colunas, tipagem básica | `ephemeral` (sem tabela física) |
| `dim` | Lógica de negócio, limpeza, joins | `table` |
| `fct` | Métricas e eventos | `incremental` |
| `mart` | Produto final para consumo por BI/analytics | `table` |

---

## 2. Fontes de Dados

Definidas em [`models/sources.yml`](models/sources.yml). Todas no schema `RAW` do Snowflake.

| Source | Tabela física | Descrição |
|---|---|---|
| `airbnb.listings` | `raw_listings` | Anúncios de hospedagem |
| `airbnb.hosts` | `raw_hosts` | Dados dos anfitriões |
| `airbnb.reviews` | `raw_reviews` | Avaliações dos hóspedes |

A source `reviews` tem **freshness** configurada: um aviso é emitido se os dados tiverem mais de 1 hora sem atualização. Isso é importante porque `fct_reviews` é incremental — dados atrasados na fonte causariam lacunas silenciosas na tabela de fatos.

```yaml
freshness:
  warn_after: {count: 1, period: hour}
```

---

## 3. Estrutura de Pastas

```
airbnb/
├── analyses/               # SQL exploratório (não materializado)
├── macros/                 # Funções Jinja reutilizáveis
├── models/
│   ├── src/                # Staging: leitura direta das sources
│   ├── dim/                # Dimensões limpas + variantes SCD
│   ├── fct/                # Tabelas de fatos
│   ├── mart/               # Modelos de consumo final
│   ├── schema.yml          # Testes e documentação de todos os modelos
│   └── sources.yml         # Declaração das fontes de dados
├── seeds/                  # Arquivos CSV versionados no repositório
├── snapshots/              # SCD Type 2 nativo do dbt
├── tests/                  # Testes singulares e genéricos customizados
├── profiles.yml            # Credenciais via env_var() — NÃO commitado
├── profiles.yml.example    # Template público para configuração
└── dbt_project.yml         # Configuração global do projeto
```

---

## 4. Camadas de Modelos

### 4.1 `src` — Staging (`ephemeral`)

**Arquivos:** [`src_listings.sql`](models/src/src_listings.sql) · [`src_hosts.sql`](models/src/src_hosts.sql) · [`src_reviews.sql`](models/src/src_reviews.sql)

A camada `src` é a única que usa `source()` para ler dados brutos. Ela apenas renomeia colunas para nomes padronizados (`id → listing_id`, `date → review_date`, `comments → review_text`) e faz a seleção das colunas relevantes.

**Por que `ephemeral`?** Modelos ephemeral não geram objetos físicos no warehouse — eles são compilados como CTEs inline nos modelos que os referenciam. Isso evita tabelas de staging desnecessárias e reduz custo de storage, mantendo ainda o benefício de modularidade no código.

```
raw_listings (source) → src_listings (ephemeral CTE) → dim_listings_cleansed (table)
```

---

### 4.2 `dim` — Dimensões (`table`)

**Dimensões base:**

| Modelo | Descrição |
|---|---|
| [`dim_listings_cleansed`](models/dim/dim_listings_cleansed.sql) | Listings com `minimum_nights` corrigido (0→1) e `price` convertido de string para NUMBER |
| [`dim_hosts_cleansed`](models/dim/dim_hosts_cleansed.sql) | Hosts com nomes nulos substituídos por `'Anonymous'`. Tem **contrato de dados** ativo |
| [`dim_listing_w_hosts`](models/dim/dim_listing_w_hosts.sql) | Dimensão desnormalizada: join entre listings e hosts. Facilita queries analíticas que precisam de ambos |

**Por que materializar como `table`?** Dimensões são consultadas com alta frequência por joins em queries de produção. Materializar como tabela evita recomputar a lógica de limpeza a cada query.

**Contrato de dados em `dim_hosts_cleansed`:**
```yaml
config:
  contract:
    enforced: true
```
Com `contract: enforced`, o dbt valida que o schema produzido em runtime bate exatamente com os tipos declarados no `schema.yml`. Se uma migração de fonte alterar um tipo de coluna silenciosamente, o dbt falha antes de sobrescrever a tabela — proteção contra breaking changes invisíveis.

---

### 4.3 `fct` — Fatos (`incremental`)

**Arquivo:** [`fct_reviews.sql`](models/fct/fct_reviews.sql)

Tabela de fatos de avaliações. Usa estratégia **incremental**: a cada run, apenas os registros com `review_date` maior que o máximo já presente na tabela são inseridos.

```sql
{% if is_incremental() %}
  AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}
```

**Por que incremental?** A tabela de reviews cresce continuamente. Reprocessar todo o histórico a cada run seria inviável em produção — tanto em tempo de execução quanto em custo de compute no Snowflake.

**`on_schema_change='fail'`:** Se uma coluna for adicionada ou removida na source, o dbt aborta o run em vez de criar silenciosamente um schema inconsistente entre os registros antigos e novos.

---

### 4.4 `mart` — Consumo (`table`)

**Arquivo:** [`mart_fullmoon_reviews.sql`](models/mart/mart_fullmoon_reviews.sql)

Camada final consumida por ferramentas de BI e analistas. Combina `fct_reviews` com o seed `seed_full_moon_dates` para classificar cada avaliação em `'full moon'` ou `'not full moon'`, baseado em se foi escrita no dia seguinte a uma lua cheia.

```sql
ON (TO_DATE(r.review_date) = DATEADD(DAY, 1, fm.full_moon_date))
```

A lógica de `DATEADD(DAY, 1, ...)` reflete a hipótese de negócio: o impacto da lua cheia nas avaliações seria sentido no dia seguinte, não no dia exato.

---

## 5. SCD Types

Slowly Changing Dimensions (SCDs) resolvem o problema de **rastrear mudanças em dados dimensionais ao longo do tempo**. Por exemplo: um anfitrião que muda o preço do listing — como registrar isso sem perder o histórico?

O projeto implementa as quatro estratégias clássicas usando `dim_listings` como sujeito:

### Type 1 — Overwrite [`dim_listings_scd1`](models/dim/dim_listings_scd1.sql)
Sem histórico. Cada run sobrescreve o valor anterior. Simples, mas não responde "qual era o preço em janeiro?".

**Quando usar:** dados descartáveis, dashboards que mostram apenas o estado atual.

### Type 2 — Add New Row [`dim_listings_scd2`](models/dim/dim_listings_scd2.sql)
Nova linha para cada mudança, com `valid_from` / `valid_to` e `is_current`. Responde qualquer pergunta histórica.

```
listing_id | price | valid_from  | valid_to    | is_current
1001       | $120  | 2024-01-01  | 2024-06-15  | FALSE
1001       | $150  | 2024-06-15  | NULL        | TRUE
```

**Quando usar:** auditoria completa, análises point-in-time. Alimentado pelo snapshot `scd_raw_listings`.

### Type 3 — Add New Column [`dim_listings_scd3`](models/dim/dim_listings_scd3.sql)
Somente o estado anterior é mantido em colunas adicionais (`previous_price`, `current_price`). Uma linha por listing (sempre atual). Inclui `price_has_changed` calculado automaticamente.

```
listing_id | current_price | previous_price | price_has_changed
1001       | $150          | $120           | TRUE
```

**Quando usar:** relatórios simples de "mudou ou não mudou", sem necessidade de histórico profundo.

### Type 4 — Separate History Table [`dim_listings_scd4`](models/dim/dim_listings_scd4.sql) + [`dim_listings_scd4_history`](models/dim/dim_listings_scd4_history.sql)
Divide em duas tabelas: a dimensão principal mantém apenas o registro atual (leve, rápida), e uma tabela de histórico separada armazena todas as versões. Join via `history_key`.

**Quando usar:** a maioria das queries precisa apenas do estado atual (performance), mas auditoria pontual ainda é necessária. Melhor trade-off entre performance e rastreabilidade.

---

## 6. Snapshots

**Arquivos:** [`raw_listings_snapshot.yml`](snapshots/raw_listings_snapshot.yml) · [`raw_hosts_snapshot.yml`](snapshots/raw_hosts_snapshot.yml)

Os snapshots são a base do **SCD Type 2 nativo do dbt**. Eles leem diretamente das sources e registram cada versão detectada, adicionando as colunas de controle: `dbt_scd_id`, `dbt_valid_from`, `dbt_valid_to`, `dbt_updated_at`.

```yaml
strategy: timestamp       # detecta mudanças comparando o campo updated_at
unique_key: id
hard_deletes: invalidate  # registros deletados na fonte são marcados como inválidos
```

`hard_deletes: invalidate` é importante: sem isso, um listing removido do Airbnb permaneceria como `is_current = TRUE` na snapshot indefinidamente — gerando dados incorretos silenciosamente.

Os modelos SCD Type 2, 3 e 4 são todos derivados desses snapshots via `ref('scd_raw_listings')`.

---

## 7. Seeds

**Arquivo:** [`seeds/seed_full_moon_dates.csv`](seeds/seed_full_moon_dates.csv)

CSV com datas de lua cheia versionado diretamente no repositório. Usado pelo mart `mart_fullmoon_reviews`.

**Por que seed e não uma tabela?** As datas de lua cheia são dados estáticos de referência, determinísticos e pequenos. Versionar no repositório garante que o dado seja reproduzível em qualquer ambiente (dev, prod, CI) sem depender de uma carga externa.

---

## 8. Macros

Macros são **funções Jinja reutilizáveis** que evitam repetição de lógica SQL entre modelos. Estão em [`macros/`](macros/).

| Macro | Assinatura | Uso |
|---|---|---|
| [`no_nulls_in_columns`](macros/no_nulls_in_columns.sql) | `(model)` | Gera um `SELECT` que retorna linhas com qualquer coluna nula. Útil em testes singulares de qualidade. |
| [`safe_divide`](macros/safe_divide.sql) | `(numerator, denominator)` | Divisão que retorna `NULL` em vez de erro quando o denominador é 0 ou nulo. |
| [`is_weekend`](macros/is_weekend.sql) | `(date_column)` | Retorna `TRUE` se a data cair em sábado ou domingo. Relevante para análises de padrão de reservas. |
| [`clean_whitespace`](macros/clean_whitespace.sql) | `(column)` | Faz `TRIM` e colapsa espaços internos múltiplos. Útil para normalizar nomes de hosts e listings. |

**Exemplo de uso em SQL:**
```sql
SELECT
    {{ clean_whitespace('host_name') }}               AS host_name,
    {{ safe_divide('total_revenue', 'total_nights') }} AS avg_revenue_per_night,
    {{ is_weekend('review_date') }}                    AS is_weekend_review
FROM {{ ref('fct_reviews') }}
```

**Por que macros em vez de SQL inline?** Se a lógica de `safe_divide` precisar mudar (ex: retornar `0` em vez de `NULL`), a alteração ocorre em um único lugar — a macro — e todos os modelos que a usam são atualizados automaticamente no próximo `dbt compile`.

---

## 9. Testes

O projeto usa os quatro tipos de teste do dbt:

### Testes de Schema (declarativos)
Definidos no [`schema.yml`](models/schema.yml). Executam automaticamente sobre as colunas declaradas.

```yaml
- name: listing_id
  data_tests:
    - not_null
    - unique
    - relationships:
        to: ref('dim_hosts_cleansed')
        field: host_id
```

### Teste Singular
**Arquivo:** [`tests/dim_listings_minimum_nights.sql`](tests/dim_listings_minimum_nights.sql)

Retorna registros com `minimum_nights < 1`. Se retornar qualquer linha, o teste falha. Testa a regra de negócio de que toda listagem deve exigir pelo menos 1 noite.

### Teste Genérico Customizado
**Arquivo:** [`tests/generic/postivie_values.sql`](tests/generic/postivie_values.sql)

Implementa o teste `positive_values` reutilizável. Aplicado em `minimum_nights` de `dim_listings_cleansed` via `schema.yml`. Generaliza a validação "o valor deve ser positivo" para qualquer modelo e coluna.

```sql
{% test positive_values(model, column_name) %}
  SELECT * FROM {{ model }} WHERE {{ column_name }} <= 0
{% endtest %}
```

### Unit Test
**Arquivo:** [`models/mart/unit_tests.yml`](models/mart/unit_tests.yml)

Testa a lógica de `mart_fullmoon_reviews` com dados mockados, sem tocar no banco de dados. Valida especificamente que `DATEADD(DAY, 1, full_moon_date)` é aplicado corretamente.

```yaml
given:
  - input: ref('fct_reviews')
    rows:
      - {review_date: '2025-01-15'}   # dia seguinte à lua cheia
expect:
  rows:
    - {review_date: '2025-01-15', is_full_moon: 'full moon'}
```

**Por que os quatro tipos?** Cada um cobre uma camada diferente de confiança:
- Schema tests → contrato dos dados
- Singular tests → regras de negócio específicas
- Generic tests → validações reutilizáveis
- Unit tests → lógica de transformação isolada do banco

Falhas de teste são armazenadas no Snowflake (`data_tests: +store_failures: true` no `dbt_project.yml`), permitindo inspecionar exatamente quais registros falharam.

---

## 10. Analyses

**Arquivo:** [`analyses/full_moon_no_sleep.sql`](analyses/full_moon_no_sleep.sql)

SQL exploratório que agrega avaliações de `mart_fullmoon_reviews` por `is_full_moon` e `review_sentiment`. Não é materializado — é compilado para `target/compiled/` e executado manualmente quando necessário.

**Por que usar `analyses/` em vez de executar SQL direto?** O arquivo participa do DAG do dbt (`ref()` funciona normalmente), é versionado no repositório e pode ser compartilhado com a equipe. É a diferença entre um notebook ad-hoc e uma query auditável.

---

## 11. Como Fazer o Projeto Funcionar

### Passo 0 — Pré-requisitos de infraestrutura

#### Python
```bash
python --version   # requer 3.8+
```

#### Ambiente virtual e instalação do dbt
```bash
python -m venv venv

# Linux / macOS
source venv/bin/activate

# Windows (PowerShell)
.\venv\Scripts\Activate.ps1

pip install dbt-snowflake
dbt --version     # confirme que a instalação funcionou
```

---

### Passo 1 — Configurar o Snowflake

Execute os comandos abaixo como `ACCOUNTADMIN` no Snowflake para criar todos os objetos necessários:

```sql
-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME   = TRUE;

-- Database e schemas
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.RAW;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.DEV;
CREATE SCHEMA   IF NOT EXISTS AIRBNB.PROD;

-- Role e usuário dbt
CREATE ROLE IF NOT EXISTS TRANSFORM;
CREATE USER IF NOT EXISTS dbt
  DEFAULT_ROLE      = TRANSFORM
  DEFAULT_WAREHOUSE = COMPUTE_WH;

-- Permissões
GRANT ROLE TRANSFORM TO USER dbt;
GRANT USAGE  ON WAREHOUSE COMPUTE_WH    TO ROLE TRANSFORM;
GRANT USAGE  ON DATABASE  AIRBNB        TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.RAW    TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.DEV    TO ROLE TRANSFORM;
GRANT ALL    ON SCHEMA    AIRBNB.PROD   TO ROLE TRANSFORM;
GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.RAW TO ROLE TRANSFORM;
GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.RAW TO ROLE TRANSFORM;
```

O schema `RAW` deve conter as três tabelas de origem:

| Tabela | Colunas mínimas esperadas |
|---|---|
| `raw_listings` | `id, name, listing_url, room_type, minimum_nights, host_id, price, created_at, updated_at` |
| `raw_hosts` | `id, name, is_superhost, created_at, updated_at` |
| `raw_reviews` | `listing_id, date, reviewer_name, comments, sentiment` |

---

### Passo 2 — Autenticação por chave (Key Pair)

O projeto usa autenticação via chave privada em vez de senha. Gere o par de chaves:

```bash
# Gerar chave privada encriptada
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256 -out rsa_key.p8

# Extrair a chave pública
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Registre a chave pública no Snowflake:
```sql
-- Cole o conteúdo de rsa_key.pub sem os headers -----BEGIN/END-----
ALTER USER dbt SET RSA_PUBLIC_KEY='MIIBIjANBgkqhki...';
```

> Guarde `rsa_key.p8` em local seguro. Nunca commite esse arquivo.

---

### Passo 3 — Clonar e configurar o projeto

```bash
git clone <url-do-repositorio>
cd airbnb
```

Crie o `profiles.yml` a partir do template:
```bash
cp profiles.yml.example profiles.yml
```

Configure as variáveis de ambiente com suas credenciais reais. O valor de `SNOWFLAKE_ACCOUNT` é o identificador da sua conta Snowflake (ex: `abc12345.us-east-1`):

```bash
export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=dbt
export SNOWFLAKE_ROLE=TRANSFORM
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_DATABASE=AIRBNB
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=sua_senha_da_chave

# Conteúdo completo do arquivo rsa_key.p8
export SNOWFLAKE_PRIVATE_KEY="$(cat rsa_key.p8)"
```

> `profiles.yml` está no `.gitignore` e **nunca deve ser commitado**. Use sempre `profiles.yml.example` como referência pública.

Para persistir as variáveis entre sessões, adicione os `export` ao seu `~/.bashrc`, `~/.zshrc` ou ao `.env` do seu sistema.

---

### Passo 4 — Instalar dependências dbt

```bash
dbt deps
```

Isso instala os pacotes declarados em `packages.yml` (ex: `dbt-utils`) na pasta `dbt_packages/`.

---

### Passo 5 — Verificar conexão

```bash
dbt debug
```

Todos os checks devem aparecer como `OK`. Se algum falhar, revise as variáveis de ambiente e o acesso ao Snowflake.

---

### Passo 6 — Executar o pipeline completo

```bash
dbt build
```

O `dbt build` executa **seed → run → snapshot → test** na ordem correta do DAG. Na primeira execução:

1. Carrega `seed_full_moon_dates.csv` no Snowflake
2. Cria todos os modelos (`src` como CTEs, `dim/mart` como tabelas, `fct` como incremental)
3. Executa os snapshots SCD Type 2 (`scd_raw_listings`, `scd_raw_hosts`)
4. Roda todos os testes de schema, singulares, genéricos e unit tests

Resultado esperado:
```
Completed successfully
Done. PASS=XX WARN=0 ERROR=0 SKIP=0 TOTAL=XX
```

---

### Passo 7 — Visualizar documentação e lineage

```bash
dbt docs generate
dbt docs serve
```

Abre `http://localhost:8080` com documentação interativa e o grafo de linhagem completo de todos os modelos.

---

### Referência rápida de comandos

```bash
dbt seed                           # Recarrega seeds
dbt run                            # Só os modelos (sem testes/snapshots)
dbt snapshot                       # Atualiza snapshots SCD Type 2
dbt test                           # Todos os testes
dbt test --select test_type:unit   # Só unit tests
dbt source freshness               # Verifica frescor das sources
dbt run --select dim+              # Camada dim e todos os dependentes
dbt run --select +mart_fullmoon_reviews  # Um modelo e todos os seus ancestrais
dbt build --target prod            # Pipeline em produção (schema PROD)
dbt clean                          # Remove target/ e dbt_packages/
```

### Targets disponíveis

| Target | Schema Snowflake | Threads | Uso |
|---|---|---|---|
| `dev` (padrão) | `DEV` | 4 | Desenvolvimento local |
| `prod` | `PROD` | 8 | Pipeline de produção |
