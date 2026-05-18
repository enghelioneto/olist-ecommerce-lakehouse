# Olist Lakehouse - Plano Mestre do Projeto

## 1. Visao Geral

Este projeto e um pipeline lakehouse end-to-end usando o dataset publico da Olist, Databricks Free Edition, PySpark, SQL, Delta Lake e arquitetura medallion.

O objetivo e construir um portfolio forte para vagas de Analytics Engineer ou Data Engineer pleno, mostrando dominio pratico de ingestao, transformacao, qualidade de dados, modelagem analitica, queries de negocio, dashboards e comunicacao tecnica.

O projeto tambem e uma trilha de aprendizado. A ideia nao e que o codigo seja escrito automaticamente de uma vez, mas que cada etapa seja entendida, discutida, implementada e revisada como em um projeto real.

Stack principal:

- Databricks Free Edition
- PySpark
- SQL
- Delta Lake
- Unity Catalog
- Medallion Architecture
- Databricks AI/BI Dashboards
- GitHub para documentacao e apresentacao

## 2. Arquitetura

Fluxo principal:

```text
CSV bruto do Kaggle
    -> Databricks Volume
    -> Bronze Delta Tables
    -> Silver Delta Tables
    -> Gold Analytical Marts
    -> SQL Analytics / Dashboard
    -> README + explicacao de entrevista
```

Papel de cada etapa:

| Etapa | Papel no projeto |
| --- | --- |
| CSV Kaggle | Dados brutos baixados da fonte publica. |
| Databricks Volume | Area de landing para armazenar os arquivos CSV no Databricks. |
| Bronze | Ingestao raw em Delta, com metadados tecnicos, hash, batch e auditoria. |
| Silver | Dados limpos, tipados, padronizados e validados por entidade. |
| Gold | Facts, dimensions e marts analiticos com granularidade clara. |
| SQL Analytics | Queries de negocio para responder perguntas relevantes. |
| Dashboard | Visualizacao executiva e exploratoria no Databricks. |
| Documentacao | Explicacao do projeto, decisoes tecnicas e narrativa de entrevista. |

Por que usar Delta Lake e medallion:

- Delta traz tabelas transacionais, schema evolution, historico e maior confiabilidade.
- Bronze preserva a rastreabilidade da origem.
- Silver concentra limpeza e padronizacao.
- Gold entrega dados prontos para analise, dashboard e consumo de negocio.

## 3. Forma De Trabalho

Este projeto sera conduzido como uma simulacao realista de lakehouse, mas com foco didatico.

Combinados de trabalho:

- Codex deve guiar, revisar, explicar e propor boas praticas.
- O codigo deve ser construido passo a passo, com entendimento do motivo de cada decisao.
- Evitar criar abstracoes ou helpers desnecessarios.
- Preferir clareza, rastreabilidade e padroes simples antes de otimizar.
- Cada notebook deve ter um papel claro dentro do pipeline.
- Sempre separar decisao tecnica de regra de negocio.

Foco de aprendizado:

- Ingestao incremental e idempotencia.
- Uso correto de metadados tecnicos.
- Tipagem e padronizacao com PySpark.
- Regras de qualidade e integridade.
- Modelagem dimensional e granularidade.
- Escrita de queries analiticas.
- Comunicacao do projeto em README, docs e entrevista.

## 4. Estado Atual Do Projeto

| Area | Status | Observacao |
| --- | --- | --- |
| Download dos dados | Feito | Dataset Olist baixado do Kaggle. |
| Organizacao local | Feito | Arquivos estao em `data/raw/olist`. |
| Upload para Databricks | Feito | Arquivos enviados para o Volume. |
| Catalog, schemas e volume | Feito | Setup criado em `00_setup_catalog_schema_volume.sql`. |
| Bronze | Feito, em revisao fina | Ingestao com Delta, append, `_row_hash`, `_batch_id`, `_ingested_at` e audit. |
| Silver | Iniciado | Precisa ser revisado e alinhado ao novo padrao da Bronze. |
| Data quality | Pendente | Notebook existe, mas ainda sera implementado. |
| Gold | Pendente | Notebook existe, mas ainda sera implementado. |
| Queries analiticas | Pendente | Arquivos SQL existem como placeholders. |
| Dashboard | Pendente | Sera criado depois da Gold. |
| Documentacao final | Em andamento | README existe; docs ainda serao preenchidos. |

## 5. Roadmap Por Fases

### Fase 0 - Dataset E Organizacao

Objetivo: preparar os arquivos brutos da Olist.

Entregaveis:

- Dataset baixado do Kaggle.
- CSVs organizados localmente.
- Arquivos brutos nao versionados no Git quando forem grandes ou sensiveis.

Checklist:

- [x] Baixar dataset Olist no Kaggle.
- [x] Conferir lista de arquivos CSV.
- [x] Organizar em `data/raw/olist`.
- [x] Registrar origem do dataset no README.

### Fase 1 - Setup Lakehouse

Objetivo: criar a estrutura base no Databricks.

Entregaveis:

- Catalog `olist_lakehouse`.
- Schemas `raw`, `bronze`, `silver`, `gold`, `quarantine` e `metadata`.
- Volume de landing para os CSVs.

Checklist:

- [x] Criar catalog.
- [x] Criar schemas.
- [x] Criar volume.
- [x] Validar upload dos arquivos no Databricks.

### Fase 2 - Bronze Incremental

Objetivo: carregar os CSVs em tabelas Delta Bronze com rastreabilidade e idempotencia.

Decisoes tecnicas:

- Ler CSVs mantendo colunas como string.
- Gravar em Delta com `append`.
- Usar `_row_hash` para identificar linhas ja carregadas.
- Usar `_batch_id` ordenavel com timestamp + sufixo aleatorio.
- Usar `_ingested_at` fixo por execucao de batch.
- Registrar auditoria em `metadata.ingestion_audit`.

Checklist:

- [x] Criar funcao de leitura dos CSVs.
- [x] Adicionar metadados tecnicos.
- [x] Calcular `_row_hash` apenas com colunas originais.
- [x] Evitar duplicidade com `left_anti` por `_row_hash`.
- [x] Gravar audit com linhas lidas, inseridas e ignoradas.
- [x] Remover dependencia de `cache`, pois serverless nao suporta.

Ponto de entrevista:

> A Bronze foi desenhada para preservar a origem e simular ingestao incremental. Mesmo com dataset estatico, o pipeline evita duplicar dados em reexecucoes usando hash da linha.

### Fase 3 - Silver

Objetivo: transformar dados raw em entidades confiaveis, tipadas e padronizadas.

Principios:

- Silver nao deve ser apenas copia da Bronze.
- Cada tabela deve representar uma entidade limpa.
- Transformacoes devem ser explicitas e simples.
- Regras de qualidade basicas devem estar claras.

Tarefas principais:

- Atualizar metadados para usar `_ingested_at`, nao `_ingested_at_utc`.
- Corrigir nomes de colunas inconsistentes, como `product_name_lenght`.
- Converter tipos numericos, datas e timestamps.
- Padronizar strings com `trim`, `lower` e `upper`.
- Remover duplicidades com chaves naturais.
- Tratar nulos com regra por tabela.
- Validar integridade entre tabelas principais.

Entidades esperadas:

- `silver.customers`
- `silver.orders`
- `silver.order_items`
- `silver.order_payments`
- `silver.order_reviews`
- `silver.geolocation`
- `silver.products`
- `silver.sellers`
- `silver.product_category_translation`

Checklist:

- [ ] Revisar uma tabela por vez.
- [ ] Definir chave natural de cada tabela.
- [ ] Definir tipos corretos por coluna.
- [ ] Definir regras de nulos.
- [ ] Padronizar categorias e textos.
- [ ] Adicionar `_silver_processed_at`.
- [ ] Conferir contagem final por tabela.

Ponto de entrevista:

> A Silver concentra a curadoria: tipagem, padronizacao, deduplicacao e validacoes. Isso evita que a camada Gold carregue regras de limpeza espalhadas.

### Fase 4 - Data Quality Checks

Objetivo: criar validacoes simples, objetivas e explicaveis.

Tipos de checks:

- Nao nulo em chaves primarias.
- Unicidade de chaves naturais.
- Integridade referencial entre tabelas.
- Valores numericos nao negativos.
- Datas em ordem logica.
- Categorias esperadas quando aplicavel.

Exemplos:

- `orders.customer_id` deve existir em `customers`.
- `order_items.order_id` deve existir em `orders`.
- `order_items.product_id` deve existir em `products`.
- `payment_value` deve ser maior ou igual a zero.
- `order_delivered_customer_date` nao deve ser anterior a `order_purchase_timestamp`.

Checklist:

- [ ] Criar checks por tabela.
- [ ] Separar checks de erro e checks de alerta.
- [ ] Registrar resultados de qualidade.
- [ ] Documentar regras em linguagem de negocio.

Ponto de entrevista:

> As regras de qualidade foram escolhidas para proteger as relacoes analiticas mais importantes: pedidos, clientes, produtos, pagamentos e entregas.

### Fase 5 - Gold

Objetivo: modelar tabelas analiticas com granularidade clara para consumo por SQL e dashboard.

Tabelas propostas:

| Tabela | Grain | Objetivo |
| --- | --- | --- |
| `gold.fact_orders` | 1 linha por pedido | Medir status, datas, cliente e desempenho de entrega. |
| `gold.fact_order_items` | 1 linha por item do pedido | Medir receita, frete, produto e seller. |
| `gold.fact_payments` | 1 linha por pagamento do pedido | Medir valor pago, parcelas e tipo de pagamento. |
| `gold.dim_customers` | 1 linha por cliente | Analisar geografia e cliente. |
| `gold.dim_products` | 1 linha por produto | Analisar categoria, dimensoes e atributos. |
| `gold.dim_sellers` | 1 linha por seller | Analisar seller e localizacao. |
| `gold.dim_dates` | 1 linha por data | Apoiar analises temporais. |
| `gold.mart_sales_daily` | 1 linha por dia | Receita, pedidos e ticket medio diario. |
| `gold.mart_sales_by_state` | 1 linha por estado | Receita, pedidos e clientes por estado. |
| `gold.mart_delivery_performance` | 1 linha por periodo/estado | Prazos, atrasos e performance de entrega. |
| `gold.mart_customer_reviews` | 1 linha por recorte analitico | Reviews, nota media e relacao com atraso. |

Checklist:

- [ ] Definir grain antes de escrever cada tabela.
- [ ] Separar facts, dimensions e marts.
- [ ] Evitar metricas duplicadas com definicoes diferentes.
- [ ] Documentar origem das colunas.
- [ ] Validar contagens e joins principais.

Ponto de entrevista:

> A Gold foi pensada para consumo analitico. Facts preservam eventos e valores; dimensions trazem contexto; marts respondem perguntas de negocio com menor complexidade para o usuario final.

### Fase 6 - Queries Analiticas

Objetivo: criar queries SQL que respondam perguntas de negocio.

Perguntas iniciais:

- Qual e a receita mensal?
- Quais estados geram mais receita?
- Qual e o ticket medio?
- Quantos pedidos foram entregues?
- Qual e o prazo medio de entrega?
- Quais categorias geram mais receita?
- Quais produtos geram mais receita?
- Existe relacao entre atraso e review score?

Checklist:

- [ ] Criar queries em `sql/analytical_queries.sql`.
- [ ] Criar queries separadas para dashboard em `sql/dashboard_queries.sql`.
- [ ] Nomear metricas de forma consistente.
- [ ] Validar resultados contra tabelas Gold.

### Fase 7 - Dashboard

Objetivo: construir dashboard no Databricks com secoes claras para negocio.

Paginas ou secoes propostas:

1. Visao geral
   - Receita total
   - Pedidos entregues
   - Ticket medio
   - Clientes unicos

2. Vendas ao longo do tempo
   - Receita mensal
   - Pedidos mensais
   - Ticket medio mensal

3. Analise geografica
   - Receita por estado
   - Pedidos por estado
   - Prazo medio de entrega por estado

4. Produtos e categorias
   - Top categorias por receita
   - Top produtos por receita
   - Frete medio por categoria

5. Experiencia do cliente
   - Media de review score
   - Reviews por estado
   - Relacao entre atraso e nota de avaliacao

Checklist:

- [ ] Criar queries base do dashboard.
- [ ] Definir metricas principais.
- [ ] Montar visualizacoes no Databricks.
- [ ] Tirar screenshots para o README, se fizer sentido.
- [ ] Documentar interpretacoes dos graficos.

### Fase 8 - Documentacao E Entrevista

Objetivo: transformar o projeto em material apresentavel para GitHub e entrevistas.

Documentos:

- `README.md`: vitrine do projeto.
- `docs/project_plan.md`: roteiro mestre.
- `docs/architecture.md`: arquitetura e decisoes.
- `docs/modeling_decisions.md`: decisoes de Silver e Gold.
- `docs/data_dictionary.md`: dicionario das tabelas finais.
- `docs/interview_explanation.md`: narrativa para entrevista.

Checklist:

- [ ] Atualizar README com arquitetura, stack, como rodar e resultados.
- [ ] Documentar decisoes importantes.
- [ ] Criar dicionario de dados das tabelas Gold.
- [ ] Escrever explicacao curta para entrevista.
- [ ] Preparar pontos de tradeoff e melhorias futuras.

## 6. Plano Silver

Regras por tema:

| Tema | Diretriz |
| --- | --- |
| Nomes | Usar nomes consistentes e corrigir erros de origem quando necessario. |
| Tipos | Converter na Silver, nao na Bronze. |
| Datas | Usar timestamps para eventos e datas para agregacoes. |
| Nulos | Definir regra por coluna, evitando preencher sem justificativa. |
| Duplicidades | Remover por chave natural documentada. |
| Categorias | Padronizar caixa e espacos; traduzir categorias quando aplicavel. |
| Integridade | Validar relacoes entre entidades antes da Gold. |
| Metadados | Preservar `_source_file`, `_source_system` e `_ingested_at`. |

Primeira revisao tecnica da Silver:

- Trocar referencias de `_ingested_at_utc` para `_ingested_at`.
- Remover `display` solto antes de gravacoes finais.
- Revisar uma tabela por vez para evitar mudancas grandes demais.
- Adicionar comentarios apenas onde a regra nao for obvia.

## 7. Plano Gold

Antes de escrever cada tabela Gold, definir:

- Grain.
- Fonte Silver.
- Chave primaria ou chave natural.
- Chaves estrangeiras.
- Metricas.
- Regras de filtro.
- Uso esperado no dashboard.

Ordem recomendada:

1. Dimensions: customers, products, sellers, dates.
2. Facts: orders, order_items, payments.
3. Marts: sales daily, sales by state, delivery performance, customer reviews.

Boas praticas:

- Evitar misturar granularidades diferentes na mesma fact.
- Evitar metricas agregadas dentro de dimensions.
- Preferir marts para metricas prontas de dashboard.
- Documentar definicoes como receita, ticket medio e atraso.

## 8. Perguntas Analiticas E Dashboard

Metricas candidatas:

| Metrica | Definicao inicial |
| --- | --- |
| Receita | Soma de `price` ou `payment_value`, com definicao final documentada. |
| Frete | Soma ou media de `freight_value`. |
| Ticket medio | Receita dividida por quantidade de pedidos. |
| Pedidos entregues | Pedidos com status `delivered`. |
| Prazo de entrega | Diferenca entre compra e entrega ao cliente. |
| Atraso | Entrega posterior a data estimada. |
| Review score medio | Media de `review_score`. |

Decisao pendente para discutir antes da Gold:

- Definir se a receita principal do dashboard vira soma de itens (`price`) ou soma de pagamentos (`payment_value`).

## 9. Criterios De Sucesso

O projeto sera considerado bem sucedido quando:

- O pipeline rodar do raw ate gold.
- Bronze for rastreavel, incremental e idempotente.
- Silver tiver tipos, nomes e regras consistentes.
- Checks de qualidade identificarem problemas reais.
- Gold tiver granularidade clara.
- Queries responderem perguntas de negocio.
- Dashboard contar uma historia analitica.
- README for claro para recrutadores.
- A explicacao de entrevista demonstrar decisoes tecnicas, tradeoffs e aprendizado.

## 10. Como Explicar Em Entrevista

Narrativa curta:

> Desenvolvi um lakehouse end-to-end no Databricks Free Edition usando o dataset Olist. O pipeline parte de CSVs brutos, carrega Bronze em Delta com metadados e idempotencia, transforma dados na Silver com tipagem e padronizacao, modela tabelas Gold para analise e cria queries e dashboard para responder perguntas de negocio.

Pontos fortes para destacar:

- Migracao de experiencia backend/fullstack para dados.
- Cuidado com rastreabilidade e idempotencia na Bronze.
- Separacao clara entre Bronze, Silver e Gold.
- Modelagem analitica com grain definido.
- Qualidade de dados e integridade referencial.
- Comunicacao tecnica via README e docs.

Tradeoffs para saber explicar:

- Por que Bronze mantem dados como string.
- Por que Silver faz tipagem e limpeza.
- Por que Gold separa facts, dimensions e marts.
- Por que simular incremental mesmo com dataset estatico.
- Por que usar Databricks Free Edition e Delta Lake.

## 11. Proximas Acoes

Proxima etapa tecnica:

1. Revisar `02_silver_transformations.py` tabela por tabela.
2. Alinhar metadados para `_ingested_at`.
3. Definir regras simples de nulos e duplicidades.
4. Validar contagens e chaves naturais.
5. Documentar as decisoes em `docs/modeling_decisions.md`.

Regra de trabalho para as proximas etapas:

- Antes de escrever codigo, discutir a intencao da tabela.
- Escrever ou alterar um bloco pequeno por vez.
- Rodar validacoes simples.
- Registrar a decisao quando ela for relevante para entrevista.
