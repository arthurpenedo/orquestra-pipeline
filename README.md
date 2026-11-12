# orquestra-pipeline

[![CI](https://github.com/arthurpenedo/orquestra-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/arthurpenedo/orquestra-pipeline/actions/workflows/ci.yml)

Orquestração com **Prefect** de um pipeline de dados em camadas (bronze → prata → ouro) sobre DuckDB, com checagens de qualidade que **derrubam o fluxo** se o dado sair errado. Dados 100% sintéticos, tudo gratuito, sem servidor.

## O problema
Um pipeline que roda "na mão" não diz qual etapa falhou, não tenta de novo e não bloqueia dado ruim. Vagas de dados pedem orquestração (Airflow, Prefect, Dagster): agendar, tentar de novo, registrar o tempo e falhar de forma visível. O [pipeline-consumidor](https://github.com/arthurpenedo/pipeline-consumidor) mostra as camadas com dbt; este projeto mostra a camada de orquestração.

## O que faz
- `etapas.py`: funções puras de cada camada (bronze, prata, ouro) e `checar_qualidade` (4 regras: id sem duplicata, nota de 1 a 5, percentual de 0 a 100, soma do ouro igual à da prata).
- `fluxo.py`: cada etapa vira uma `@task` do Prefect (a carga da bronze tem 2 tentativas) dentro de um `@flow`; o tempo de cada etapa é registrado e devolvido.
- `cli.py`: `python -m orquestra_pipeline rodar` gera dados sintéticos e roda o fluxo.

## Números medidos (rodada local, 5.000 linhas sintéticas)
| Etapa | Tempo |
|---|---|
| bronze | 0,994 s |
| prata | 0,083 s |
| ouro | 0,055 s |
| qualidade | 0,042 s |

8 testes passando (5 das etapas, 3 do fluxo e da CLI; a suíte leva cerca de 65 s por causa do servidor temporário do Prefect).

## Como rodar
```
pip install -e ".[dev]"
python -m orquestra_pipeline rodar --linhas 5000
python -m pytest -q
```

### Rodar o dbt do pipeline-consumidor e agendar
```
pip install -e ../pipeline-consumidor
python -m orquestra_pipeline dbt --csv ../pipeline-consumidor/dados/amostra_sintetica.csv
python -m orquestra_pipeline dbt --csv <csv> --agendar "0 6 * * *"   # serve o fluxo todo dia às 6h (Ctrl+C para parar)
```
O `dbt build` é uma tarefa com 1 retry; se algum teste do dbt falhar, o fluxo falha. 13 testes, sem rede (o dbt e o `serve` são simulados nos testes).

## Limites honestos
- O agendamento (`flow.serve`) mantém um processo aberto; não há deploy em servidor. O `serve` e o dbt real não foram executados na rotina (testes usam simulação).
- Os dados são sintéticos; o fluxo aceita um CSV próprio (`--csv`) com as colunas `id, empresa, data, status, nota`.
- Não é Airflow (que não roda nativamente no Windows); os conceitos (tarefas, tentativas, dependências, falha visível) são os mesmos.

MIT.
