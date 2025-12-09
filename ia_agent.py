#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agente de IA para análise do banco Contele
John - Analista de Dados Sênior da TecnoTop Automação

Versão blindada + memória:
- Contexto temporal dinâmico
- Validações reforçadas
- Retry com backoff
- Logs estruturados
- Detecção de SQL genérico e refinamento
- Auto-teste de integridade
- Integração com filtros do dashboard (período padrão)
- Memória de conversa (histórico por sessão) para manter contexto
"""
import os
import re
import time
import json
import logging
import datetime as dt
from typing import Optional, Dict, Any, List, Tuple

import psycopg2
import psycopg2.extras
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# permite override local se existir
if os.path.exists(".env.local"):
    load_dotenv(".env.local", override=True)

# ================== CONFIGURAÇÕES BÁSICAS ==================

DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

IA_CONFIG = {
    "nome": "John",
    "papel": "Analista de Dados Sênior",
    "tom": "Profissional com senso de humor aguçado, refletindo o estilo do usuário João sem perder objetividade",
    "especialidade": (
        "análise de visitas técnicas e relacionamento com clientes B2B "
        "nos setores industrial e comercial. Especialista em produtos/marcas "
        "(Festo, Wago, Hengst, Rexroth) com foco no mercado da Região do Rio de Janeiro"
    ),
    "empresa": "TecnoTop Automação",
}

ANO_ATUAL = dt.datetime.now().year
MES_ATUAL = dt.datetime.now().month

# ================== LOGS ==================

LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "ia_agent.log")


def init_logger():
    logger = logging.getLogger("ia_agent")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


logger = init_logger()

# ================== TABELAS / VIEWS PERMITIDAS ==================

TABELAS_PERMITIDAS = {
    "contele.contele_os",
    "contele.contele_os_all",
    "contele.contele_answers",
    "contele.contele_answers_all",
    "contele.vw_todas_os_respostas",
    "contele.vw_prospeccao",
    "contele.vw_relacionamento",
    "contele.vw_levantamento_de_necessidade",
    "contele.vw_visita_tecnica",
    "contele.vw_resumo_vendedores",
    "contele.vw_resumo_clientes",
    "contele.vw_timeline_atividades",
    "contele.vw_pendencias",
    "contele.vw_resumo_pendencias_vendedor",
    "contele.vw_resumo_pendencias_cliente",
    # adicionadas para as perguntas da supervisora
    "contele.vw_visitas_status",
    "contele.vw_portfolio_clientes",
}

# ================== MEMÓRIA DE CONVERSA ==================


def _formatar_historico(
    history: Optional[List[Dict[str, Any]]],
    max_msgs: int = 12,
    max_chars: int = 4000,
) -> str:
    """
    history esperado como lista de dicts:
    [
      {"role": "user" | "assistant", "text": "conteúdo..."},
      ...
    ]

    Retorna um texto compacto com as últimas interações
    para ser usado como contexto (NÃO como filtro SQL).
    """
    if not history:
        return ""

    ultimas = history[-max_msgs:]
    linhas: List[str] = []

    for msg in ultimas:
        role = msg.get("role", "user")
        texto = msg.get("text") or msg.get("content") or ""
        if not texto:
            continue

        prefixo = "Usuário" if role == "user" else IA_CONFIG["nome"]
        linhas.append(f"{prefixo}: {texto}")

    historico = "\n".join(linhas)
    if len(historico) > max_chars:
        historico = historico[-max_chars:]

    return historico


def get_contele_schema_info() -> str:
    """Descrição do schema para orientar a geração de SQL."""
    return f"""
# 📊 SCHEMA CONTELE (Blindado)

## 🗓 CONTEXTO TEMPORAL
Ano atual: {ANO_ATUAL}
Mês atual: {MES_ATUAL}
Regras:
- "este mês": primeiro dia de {ANO_ATUAL}-{MES_ATUAL:02d} até primeiro dia do mês seguinte
- "mês passado": mês anterior ao atual (mes_atual - 1)
- "mês de X" ou "mês X": usar ano {ANO_ATUAL} e mês X
- Se perguntar por mês sem ano: assuma ano {ANO_ATUAL}
- Nunca usar ano 2023 ou 2024 em nova consulta (a menos que explicitamente dito)

## TABELAS PRINCIPAIS (1 linha = 1 OS)
contele.contele_os(
  os,
  task_id,
  assignee_name,   -- vendedor / responsável
  poi,             -- cliente / POI
  status,
  created_at,      -- data/hora da OS (visita)
  ...
)

contele.contele_os_all(... inclui has_objetivo ...)

## TABELAS DE RESPOSTAS (várias linhas por OS)
contele.contele_answers(
  task_id,
  form_title,
  question_title,
  answer_human,
  created_at,
  ...
)

contele.contele_answers_all(...)

## VIEWS NORMALIZADAS / PIVOT / RESUMO
- contele.vw_todas_os_respostas
  • 1+ linhas por OS (task_id)
  • Campos típicos: os, task_id, poi, assignee_name, os_created_at, form_title,
    question_title, answer_human, status, objetivo (quando mapeado), etc.
  • USO PRIORITÁRIO para ler conteúdo de formulários (segmento, área visitada, sintomas, etc.)

- contele.vw_resumo_vendedores
  • Resumo por vendedor (quantidade de visitas, objetivos, etc.)

- contele.vw_resumo_clientes
  • Resumo por cliente (qtd visitas, últimos contatos, etc.)

- contele.vw_timeline_atividades
  • Linha do tempo de ações por OS / cliente

- contele.vw_pendencias
  • 1 linha por OS que gerou pendência (ou, no mínimo, 1 linha por task_id relacionada a pendência)
  • Campos típicos (confirmados): task_id, os_created_at, assignee_name, poi,
    objetivo, gerou_pendencia, linha_pendencia.
  • IMPORTANTE:
      - NÃO existe coluna de data específica da pendência (ex.: data_criacao_pendencia).
      - SEMPRE use os_created_at como referência de período para analisar pendências.
      - NÃO use colunas inexistentes como descricao_pendencia ou pendencia_aberta.

- contele.vw_resumo_pendencias_vendedor
- contele.vw_resumo_pendencias_cliente

- contele.vw_prospeccao / vw_relacionamento / vw_levantamento_de_necessidade / vw_visita_tecnica
  • Cada uma representa um conjunto de OS por tipo de visita/objetivo

- Outras views usadas no dashboard (exemplo):
  - contele.vw_visitas_status (1 linha = 1 OS com objetivo, status, vendedor, poi)
  - contele.vw_portfolio_clientes (portfólio Festo/Bosch/Hengst/Wago por cliente)

## REGRAS CRÍTICAS PARA VISITAS / OS

1. Quando a pergunta for:
   - "quantas visitas"
   - "quantas OS"
   - "quantas ordens de serviço"
   - "quantos formulários"
   por vendedor / cliente / período:

   → PRIORIZE usar **contele.contele_os** (1 linha = 1 OS):

   Exemplo:
   SELECT COUNT(*) AS total_visitas
   FROM contele.contele_os o
   WHERE o.assignee_name ILIKE '%Rafael%'
     AND o.created_at >= '2025-10-01'
     AND o.created_at <  '2025-11-01';

2. Se usar **contele.vw_todas_os_respostas** para contar visitas/OS,
   NUNCA use COUNT(*). Use SEMPRE:

   COUNT(DISTINCT task_id)  -- 1 task_id = 1 OS

   Exemplo:
   SELECT COUNT(DISTINCT task_id) AS total_visitas
   FROM contele.vw_todas_os_respostas
   WHERE assignee_name ILIKE '%Rafael%'
     AND os_created_at >= '2025-10-01'
     AND os_created_at <  '2025-11-01';

3. Resumo/detalhes de OS específica -> SEMPRE vw_todas_os_respostas.
4. Pendências -> vw_pendencias ou suas views de resumo.
   • Período de pendência = SEMPRE baseado em os_created_at.
5. Buscar objetivo/tipo de visita -> usar:
   - objetivo na própria view de visitas (ex: vw_visitas_status)
   - OU question_title ILIKE '%objetivo da visita%' em vw_todas_os_respostas.
6. SEMPRE adicionar LIMIT (<=1000).
7. Usar ILIKE para buscas textuais.
8. Evitar GROUP BY com agregações impróprias.
9. Nunca inventar tabela/coluna fora da lista.

## EXEMPLOS CHAVE

Resumo OS:
SELECT question_title, answer_human, assignee_name, status, poi, os_created_at
FROM contele.vw_todas_os_respostas
WHERE os = '5078'
ORDER BY question_title
LIMIT 100;

Pendências (simples, últimas pendências registradas):
SELECT task_id, poi, assignee_name, objetivo, os_created_at, linha_pendencia, gerou_pendencia
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
ORDER BY os_created_at DESC
LIMIT 100;

-------------------------------------------------------------------------------
## MAPA DAS PERGUNTAS DA GESTORA (como responder em SQL)

### 1) Pendências e oportunidades
Campos principais:
- Pergunta de formulário "Essa visita gerou negócio/pendência?":
  • question_title ILIKE '%essa visita gerou negócio%' OR ILIKE '%essa visita gerou pendência%'
  • answer_human contém algo como 'Negócio', 'Pendência', 'Não gerou', etc.
- Pergunta "Pendência relacionada a qual linha?":
  • question_title ILIKE '%pendência relacionada a qual linha%'
  • answer_human = 'Hidráulica', 'Pneumática', 'Elétrica/Automação', 'Service', 'Filtros', etc.
- View de pendências:
  • contele.vw_pendencias (quando quiser trabalhar diretamente com 1 linha = 1 OS que gerou pendência).
  • Período SEMPRE com base em os_created_at.

Exemplos:

1. "Quantas pendências foram geradas pelo vendedor X no período Y?"

→ Preferir vw_pendencias:

SELECT
  assignee_name,
  COUNT(*) AS total_pendencias
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
  AND assignee_name ILIKE '%NOME VENDEDOR%'
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY assignee_name
LIMIT 100;

2. "Quais linhas tiveram mais pendências no período Y?"

SELECT
  LOWER(TRIM(linha_pendencia)) AS linha,
  COUNT(*)                      AS total_pendencias
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
  AND linha_pendencia IS NOT NULL
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(linha_pendencia))
ORDER BY total_pendencias DESC
LIMIT 100;

3. "Quantas oportunidades foram geradas por cada tipo de visita no período Y?"

→ Considerar como "oportunidade" toda visita que gerou negócio ou pendência:

WITH visitas_oportunidade AS (
  SELECT DISTINCT task_id
  FROM contele.vw_todas_os_respostas
  WHERE question_title ILIKE '%essa visita gerou%'
    AND (answer_human ILIKE '%pend%' OR answer_human ILIKE '%negócio%')
)
SELECT
  v.objetivo,             -- campo objetivo/tipo de visita em vw_visitas_status
  COUNT(*) AS total_oportunidades
FROM contele.vw_visitas_status v
JOIN visitas_oportunidade o USING (task_id)
WHERE v.created_at >= '2025-10-01'
  AND v.created_at <  '2025-11-01'
GROUP BY v.objetivo
ORDER BY total_oportunidades DESC
LIMIT 100;

4. "Qual cliente gerou mais pendências no período Y?"

→ vw_pendencias agrupando por poi:

SELECT
  poi,
  COUNT(*) AS total_pendencias
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY poi
ORDER BY total_pendencias DESC
LIMIT 100;

5. "Qual o percentual de visitas que geraram pendência para o vendedor X no período Y?"

→ Numerador: qtd visitas com pendência no período (vw_pendencias).
→ Denominador: qtd total de visitas (OS) no mesmo período para o vendedor (contele_os).

WITH visitas_vendedor AS (
  SELECT
    assignee_name,
    COUNT(*) AS total_visitas
  FROM contele.contele_os
  WHERE assignee_name ILIKE '%NOME VENDEDOR%'
    AND created_at >= '2025-10-01'
    AND created_at <  '2025-11-01'
  GROUP BY assignee_name
),
visitas_com_pendencia AS (
  SELECT
    assignee_name,
    COUNT(*) AS visitas_com_pendencia
  FROM contele.vw_pendencias
  WHERE gerou_pendencia = true
    AND assignee_name ILIKE '%NOME VENDEDOR%'
    AND os_created_at >= '2025-10-01'
    AND os_created_at <  '2025-11-01'
  GROUP BY assignee_name
)
SELECT
  v.assignee_name,
  v.total_visitas,
  COALESCE(p.visitas_com_pendencia, 0) AS visitas_com_pendencia,
  CASE
    WHEN v.total_visitas > 0
    THEN ROUND(100.0 * COALESCE(p.visitas_com_pendencia, 0) / v.total_visitas, 1)
    ELSE 0
  END AS percentual_visitas_com_pendencia
FROM visitas_vendedor v
LEFT JOIN visitas_com_pendencia p
  ON v.assignee_name = p.assignee_name
LIMIT 100;

-------------------------------------------------------------------------------
### 2) Visitas concluídas, abordagens sem sucesso, taxa de sucesso

Campos chave:
- Tabela/ view de visitas: contele.vw_visitas_status ou contele.contele_os
- Formulário de "Abordagem sem sucesso":
  • form_title = 'Abordagem sem sucesso'
  • question_title: 'Situação Encontrada%' (empresa fechada, responsável ausente, recusa…)

1. "Quantas visitas concluídas o vendedor X realizou no período Y?"

SELECT
  assignee_name,
  COUNT(*) AS visitas_concluidas
FROM contele.vw_visitas_status
WHERE assignee_name ILIKE '%NOME VENDEDOR%'
  AND status ILIKE '%conclu%'
  AND created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY assignee_name
LIMIT 100;

2. "Quantas abordagens sem sucesso ocorreram no período Y e por qual motivo?"

SELECT
  LOWER(TRIM(answer_human)) AS motivo,
  COUNT(DISTINCT task_id)   AS total
FROM contele.vw_todas_os_respostas
WHERE lower(form_title) = 'abordagem sem sucesso'
  AND question_title ILIKE 'Situação Encontrada%'
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(answer_human))
ORDER BY total DESC
LIMIT 100;

3. "Qual a taxa de sucesso das visitas do vendedor X no período Y?"
   (visitas realizadas ÷ total de tentativas)

→ Total de tentativas = total de OS.
→ Visitas efetivas = total de OS - abordagens sem sucesso.

-------------------------------------------------------------------------------
### 3) Visitas por objetivo / tipo de visita

Campos:
- objetivo / tipo de visita em vw_visitas_status
- ou views: vw_prospeccao, vw_relacionamento, vw_levantamento_de_necessidade, vw_visita_tecnica.

1. "Quantas visitas foram feitas por objetivo no período Y?"

SELECT
  CASE
    WHEN status ILIKE '%abordagem sem sucesso%'
      THEN 'Abordagem sem sucesso'
    WHEN objetivo IS NULL OR TRIM(objetivo) = ''
      THEN 'Sem objetivo informado'
    ELSE objetivo
  END AS objetivo_legenda,
  COUNT(*) AS total
FROM contele.vw_visitas_status
WHERE created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY
  CASE
    WHEN status ILIKE '%abordagem sem sucesso%'
      THEN 'Abordagem sem sucesso'
    WHEN objetivo IS NULL OR TRIM(objetivo) = ''
      THEN 'Sem objetivo informado'
    ELSE objetivo
  END
ORDER BY total DESC
LIMIT 100;

2. "Qual vendedor realizou mais visitas de prospecção no período Y?"

SELECT
  assignee_name,
  COUNT(*) AS total_prospeccao
FROM contele.vw_visitas_status
WHERE objetivo ILIKE '%prospec%'
  AND created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY assignee_name
ORDER BY total_prospeccao DESC
LIMIT 100;

-------------------------------------------------------------------------------
### 4) Segmento, área visitada, com quem conversou

Campos em vw_todas_os_respostas:
- Segmento:   question_title ILIKE '%segmento do cliente%'
- Área visitada: question_title ILIKE '%área visitada%'
- Com quem conversou: question_title ILIKE '%com quem conversou%' ou '%cargo%'.

1. "Quantas visitas foram feitas para o segmento X no período Y?"

SELECT
  COUNT(DISTINCT task_id) AS total_visitas
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%segmento do cliente%'
  AND answer_human ILIKE '%SEGMENTO%'
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
LIMIT 100;

2. "Quais segmentos receberam menos visitas no período Y?"

SELECT
  LOWER(TRIM(split_part(answer_human, ' - ', 1))) AS segmento,
  COUNT(DISTINCT task_id)                          AS qtd_visitas
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%segmento do cliente%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(split_part(answer_human, ' - ', 1)))
ORDER BY qtd_visitas ASC
LIMIT 100;

3. "Qual a área mais visitada dentro dos clientes no período Y?"
   (produção, manutenção, engenharia, compras)

SELECT
  LOWER(TRIM(answer_human)) AS area_visitada,
  COUNT(DISTINCT task_id)   AS qtd_visitas
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%área visitada%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(answer_human))
ORDER BY qtd_visitas DESC
LIMIT 100;

4. "Quantos clientes ficaram sem visita nos últimos Z dias?"

WITH clientes_visitados AS (
  SELECT DISTINCT poi
  FROM contele.contele_os
  WHERE created_at >= (CURRENT_DATE - INTERVAL '30 days')
),
todos_clientes AS (
  SELECT DISTINCT poi
  FROM contele.contele_os
)
SELECT
  COUNT(*) AS clientes_sem_visita
FROM todos_clientes c
LEFT JOIN clientes_visitados v
  ON c.poi = v.poi
WHERE v.poi IS NULL;

5. "Quais clientes receberam mais de uma visita no período Y?"

SELECT
  poi,
  COUNT(*) AS qtd_visitas
FROM contele.contele_os
WHERE created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY poi
HAVING COUNT(*) > 1
ORDER BY qtd_visitas DESC
LIMIT 100;

-------------------------------------------------------------------------------
### 5) Sintomas, causas, ações, recomendações

Campos em vw_todas_os_respostas:
- 'Sintomas relatados': question_title ILIKE '%sintomas%'
- 'Causas identificadas': question_title ILIKE '%causas identificadas%'
- 'Ações realizadas': question_title ILIKE '%ações realizadas%'
- 'Recomendações de ações futuras': question_title ILIKE '%recomenda%' OR '%ações futuras%'

1. "Quais foram os problemas mais relatados pelos clientes no período Y?"

SELECT
  LOWER(TRIM(answer_human)) AS problema,
  COUNT(DISTINCT task_id)   AS total
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%sintomas%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(answer_human))
ORDER BY total DESC
LIMIT 100;

2. "Quais causas foram mais identificadas?"
   (mecânicas, elétricas, eletrônicas, software, mau uso)

SELECT
  LOWER(TRIM(answer_human)) AS causa,
  COUNT(DISTINCT task_id)   AS total
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%causas identificadas%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(answer_human))
ORDER BY total DESC
LIMIT 100;

3. "Quantas visitas tiveram diagnóstico mecânico no período Y?"

SELECT
  COUNT(DISTINCT task_id) AS visitas_diagnostico_mecanico
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%causas identificadas%'
  AND answer_human ILIKE '%mecânic%'
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
LIMIT 100;

4. "Quantas recomendações de ações futuras foram registradas no período Y?"

SELECT
  COUNT(DISTINCT task_id) AS visitas_com_recomendacao
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%recomenda%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
LIMIT 100;

-------------------------------------------------------------------------------
### 6) Insatisfação, feedback, acompanhamento, relacionamento

Campos:
- Insatisfação: question_title ILIKE '%insatisfa%'
- Feedback:    question_title ILIKE '%feedback%'
- Acompanhamento / follow-up: question_title ILIKE '%acompanh%' OR '%follow%'
- Visitas de relacionamento: objetivo/tipo de visita = 'Relacionamento' ou similar.

1. "Quantos clientes relataram alguma insatisfação no período Y?"

SELECT
  COUNT(DISTINCT poi) AS clientes_insatisfeitos
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%insatisfa%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
LIMIT 100;

2. "Quantas visitas de relacionamento foram realizadas no período Y?"

SELECT
  COUNT(*) AS visitas_relacionamento
FROM contele.vw_visitas_status
WHERE objetivo ILIKE '%relacionamento%'
  AND created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
LIMIT 100;

3. "Quais clientes receberam mais visitas de follow-up?"

SELECT
  poi,
  COUNT(*) AS qtd_visitas_followup
FROM contele.vw_visitas_status
WHERE objetivo ILIKE '%follow%' OR objetivo ILIKE '%acompanh%'
GROUP BY poi
ORDER BY qtd_visitas_followup DESC
LIMIT 100;

-------------------------------------------------------------------------------
### 7) Prospecção, concorrentes, percepção, uso de produtos

Campos:
- Objetivo/tipo de visita = Prospecção -> vw_visitas_status ou vw_prospeccao.
- Concorrentes: question_title ILIKE '%concorrente%'
- Interesse em conhecer TecnoTop: question_title ILIKE '%conhecer a tecnotop%'
- Uso de produtos Festo/Bosch/Hengst/Wago: vw_portfolio_clientes.

1. "Qual o total de visitas de prospecção feitas pelo vendedor X?"

SELECT
  assignee_name,
  COUNT(*) AS total_prospeccao
FROM contele.vw_visitas_status
WHERE objetivo ILIKE '%prospec%'
  AND assignee_name ILIKE '%NOME VENDEDOR%'
  AND created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY assignee_name
LIMIT 100;

2. "Em quantas visitas o cliente mencionou concorrentes?"
   / "Quais concorrentes mais mencionados?"
   / "Quais concorrentes foram mais mencionados no período Y?"

SELECT
  LOWER(TRIM(answer_human)) AS concorrente,
  COUNT(DISTINCT poi)       AS qtd_clientes,
  COUNT(DISTINCT task_id)   AS qtd_visitas
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%concorrente%'
  AND answer_human IS NOT NULL
  AND answer_human <> ''
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY LOWER(TRIM(answer_human))
ORDER BY qtd_visitas DESC
LIMIT 100;

3. "Quantos clientes demonstraram interesse em conhecer a Tecnotop pela primeira vez?"

SELECT
  COUNT(DISTINCT poi) AS clientes_interessados
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE '%conhecer a tecnotop%'
  AND answer_human ILIKE '%sim%'
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
LIMIT 100;

4. "Quais clientes utilizam produtos Festo, Bosch, Hengst ou Wago?"

SELECT
  poi,
  usa_festo,
  usa_bosch_rexroth,
  usa_hengst,
  usa_wago
FROM contele.vw_portfolio_clientes
WHERE (usa_festo = true
    OR usa_bosch_rexroth = true
    OR usa_hengst = true
    OR usa_wago = true)
ORDER BY poi
LIMIT 200;

-------------------------------------------------------------------------------
### 8) Comparações entre vendedores e evolução

1. "Comparar a quantidade de visitas realizadas por cada vendedor no período Y."

SELECT
  assignee_name,
  COUNT(*) AS total_visitas
FROM contele.contele_os
WHERE created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY assignee_name
ORDER BY total_visitas DESC
LIMIT 100;

2. "Comparar pendências geradas por cada vendedor no período Y."

SELECT
  assignee_name,
  COUNT(*) AS total_pendencias
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
  AND os_created_at >= '2025-10-01'
  AND os_created_at <  '2025-11-01'
GROUP BY assignee_name
ORDER BY total_pendencias DESC
LIMIT 100;

3. "Comparar quantidade de novos clientes abordados entre os vendedores no período Y."

WITH visitas_periodo AS (
  SELECT poi, assignee_name, MIN(created_at) AS primeira_visita
  FROM contele.contele_os
  WHERE created_at >= '2025-10-01'
    AND created_at <  '2025-11-01'
  GROUP BY poi, assignee_name
)
SELECT
  assignee_name,
  COUNT(*) AS novos_clientes_abordados
FROM visitas_periodo
GROUP BY assignee_name
ORDER BY novos_clientes_abordados DESC
LIMIT 100;

4. "Comparar visitas por objetivo entre os vendedores no período Y."

SELECT
  assignee_name,
  objetivo,
  COUNT(*) AS total_visitas
FROM contele.vw_visitas_status
WHERE created_at >= '2025-10-01'
  AND created_at <  '2025-11-01'
GROUP BY assignee_name, objetivo
ORDER BY assignee_name, total_visitas DESC
LIMIT 100;

-------------------------------------------------------------------------------
### 9) Variações, crescimento e múltiplas pendências

1. "Quais motivos de abordagem sem sucesso aumentaram no último período?"
   → Comparar dois períodos (ex.: mês atual vs mês anterior) usando a mesma lógica de contagem
     por motivo da view de 'abordagem sem sucesso'.

2. "Quais segmentos mais cresceram em visitas em relação ao mês anterior?"
   → Montar CTE com contagem por segmento em dois períodos e calcular diferença/percentual.

3. "Quais visitas resultaram em mais de uma pendência?"

SELECT
  task_id,
  poi,
  assignee_name,
  COUNT(*) AS qtd_pendencias
FROM contele.vw_pendencias
WHERE gerou_pendencia = true
GROUP BY task_id, poi, assignee_name
HAVING COUNT(*) > 1
ORDER BY qtd_pendencias DESC
LIMIT 100;

4. "Quais áreas internas dos clientes geram mais oportunidades?"
   → Cruzar:
   - Áreas internas (pergunta de 'área visitada' ou 'área do contato') em vw_todas_os_respostas
   - Com visitas que geraram negócio/pendência ou com registros em vw_pendencias.

-------------------------------------------------------------------------------
USO GERAL:
- Para cada pergunta da gestora, usar estes exemplos como guia:
  • Escolher a tabela/view certa (os, vw_visitas_status, vw_todas_os_respostas, vw_pendencias, portfolio, etc.).
  • Respeitar sempre a regra: 1 OS = 1 task_id; se estiver em vw_todas_os_respostas, usar COUNT(DISTINCT task_id).
  • Aplicar filtros de período conforme regras temporais e filtros do dashboard.
"""

# ================== DETECÇÃO DO TIPO DE PERGUNTA ==================


def detectar_tipo_pergunta(pergunta: str) -> str:
    pergunta_lower = pergunta.lower().strip()

    conversas_casuais = [
        "oi",
        "olá",
        "ola",
        "hey",
        "hi",
        "hello",
        "bom dia",
        "boa tarde",
        "boa noite",
        "bom diaa",
        "tudo bem",
        "como vai",
        "como está",
        "beleza",
        "e aí",
        "eai",
        "obrigado",
        "obrigada",
        "valeu",
        "vlw",
        "brigadão",
        "brigado",
        "tchau",
        "até logo",
        "falou",
        "até mais",
        "flw",
        "legal",
        "bacana",
        "show",
        "top",
        "massa",
        "dahora",
    ]

    meta_keywords = [
        "quem é você",
        "quem você é",
        "quem voce é",
        "quem voce e",
        "o que você faz",
        "o que voce faz",
        "para que serve",
        "sua função",
        "sua individualidade",
        "se apresente",
        "seu papel",
        "sua especialidade",
        "quem és",
        "qual é seu nome",
        "qual e seu nome",
        "o que você consegue fazer",
        "suas capacidades específicas",
        "como você funciona internamente",
        "que tipo de pergunta",
    ]

    dados_keywords = [
        "quantas",
        "quantos",
        "quanto",
        "total",
        "soma",
        "média",
        "media",
        "mostre",
        "liste",
        "exiba",
        "busque",
        "encontre",
        "procure",
        "os",
        "os's",
        "visita",
        "cliente",
        "vendedor",
        "técnico",
        "tecnico",
        "poi",
        "task",
        "objetivo",
        "prospecção",
        "prospeccao",
        "relacionamento",
        "levantamento",
        "ranking",
        "top",
        "último",
        "ultima",
        "mês",
        "mes",
        "ano",
        "período",
        "periodo",
        "status",
        "concluída",
        "concluida",
        "pendente",
        "finalizada",
        "comparar",
        "comparação",
        "comparacao",
        "diferença",
        "diferenca",
        "resumo",
        "detalhes",
        "informações",
        "informacoes",
        "relata",
        "foi feito",
        "diz",
        "sobre",
        "aprofundar",
        "mais sobre",
        "essa os",
        "desta os",
        "da os",
        "essa visita",
        "esse cliente",
        "consegue",
        "pode",
        "pendência",
        "pendencias",
    ]

    if any(pergunta_lower.startswith(c) or pergunta_lower == c for c in conversas_casuais):
        return "casual"
    if any(m in pergunta_lower for m in meta_keywords):
        return "meta"
    if any(d in pergunta_lower for d in dados_keywords):
        return "dados"
    return "dados"

# ================== VALIDAÇÃO / CORREÇÃO DE SQL ==================


def _extrair_tabelas(sql: str) -> set:
    """
    Extrai candidatos de nomes usados em FROM/JOIN.
    Aqui não filtramos ainda; o filtro de 'só schema.tabela'
    fica na validação para não ferrar CTE.
    """
    candidatos = set()
    for token in re.findall(r"\bFROM\s+([a-zA-Z0-9_.]+)", sql, flags=re.IGNORECASE):
        candidatos.add(token.strip())
    for token in re.findall(r"\bJOIN\s+([a-zA-Z0-9_.]+)", sql, flags=re.IGNORECASE):
        candidatos.add(token.strip())
    return candidatos


def _tem_colunas_invalidas(sql: str) -> bool:
    """
    Bloqueia o uso de colunas que NÃO existem na base/conteúdo real
    de vw_pendencias (para evitar exatamente o erro que você tomou).
    """
    sql_lower = sql.lower()
    if "data_criacao_pendencia" in sql_lower:
        return True
    if "descricao_pendencia" in sql_lower:
        return True
    return False


def _detectar_sql_generico(sql: str) -> bool:
    padrao_limite1 = re.search(
        r"SELECT\s+.+\s+FROM\s+contele\.contele_os\s+LIMIT\s+1\b",
        sql,
        flags=re.IGNORECASE,
    )
    sem_where = "WHERE" not in sql.upper()
    sem_order = "ORDER BY" not in sql.upper()
    return bool(padrao_limite1 and sem_where and sem_order)


def _forcar_distinct_task_id_em_vw_todas(sql: str) -> str:
    """
    Se a query usar vw_todas_os_respostas e COUNT(*),
    converte para COUNT(DISTINCT task_id) para não inflar visitas.
    """
    if "contele.vw_todas_os_respostas" in sql:
        sql = re.sub(
            r"COUNT\s*\(\s*\*\s*\)",
            "COUNT(DISTINCT task_id)",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


def validar_e_corrigir_sql(sql: str) -> tuple:
    # limpa markdown e espaços
    sql_limpo = sql.strip()
    sql_limpo = sql_limpo.replace("```sql", "").replace("```", "").strip()

    # pega apenas o primeiro statement antes de ';'
    if ";" in sql_limpo:
        sql_limpo = sql_limpo.split(";", 1)[0].strip()

    # aplica DISTINCT task_id quando usar vw_todas_os_respostas
    sql_limpo = _forcar_distinct_task_id_em_vw_todas(sql_limpo)

    sql_upper = sql_limpo.upper()

    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False, "❌ SQL deve começar com SELECT ou WITH"

    comandos_bloqueados = [
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "TRUNCATE",
        "ALTER",
        "CREATE",
        "GRANT",
        "REVOKE",
    ]
    for cmd in comandos_bloqueados:
        if re.search(rf"\b{cmd}\b", sql_upper):
            return False, f"❌ Comando {cmd} não permitido"

    anos = re.findall(r"\b(20\d{2})[-/]", sql_limpo)
    for ano in anos:
        ano_int = int(ano)
        if ano_int < 2024 or ano_int > ANO_ATUAL:
            return (
                False,
                f"❌ Ano {ano_int} inválido na consulta (corrija para {ANO_ATUAL} se aplicável)",
            )

    tabelas_usadas = _extrair_tabelas(sql_limpo)

    # 🔧 PONTO CRÍTICO: só validamos nomes totalmente qualificados (schema.tabela).
    # CTEs como visitas_com_pendencia, abordagens_sem_sucesso, clientes_visitados
    # NÃO têm ponto, então não entram nessa validação.
    for t in tabelas_usadas:
        # Ignora CTE / alias / nome sem schema
        if "." not in t:
            continue
        if t not in TABELAS_PERMITIDAS:
            return False, f"❌ Referência a tabela/view não permitida: {t}"

    if _tem_colunas_invalidas(sql_limpo):
        return False, "❌ Estão sendo usadas colunas que não existem em vw_pendencias (ex.: data_criacao_pendencia). Ajuste para usar os_created_at."

    if "LIMIT" not in sql_upper:
        sql_limpo += "\nLIMIT 100"
    else:
        m = re.search(r"LIMIT\s+(\d+)", sql_upper)
        if m:
            limite = int(m.group(1))
            if limite > 1000:
                sql_limpo = re.sub(
                    r"LIMIT\s+\d+",
                    "LIMIT 1000",
                    sql_limpo,
                    flags=re.IGNORECASE,
                )

    if _detectar_sql_generico(sql_limpo):
        return False, "❌ Query muito genérica. Especifique OS, período ou objetivo."

    return True, sql_limpo

# ================== GERAÇÃO DE SQL VIA IA ==================


def _montar_bloco_filtros(filters: Optional[Dict[str, Any]]) -> str:
    """
    Constrói texto com filtros do dashboard para o prompt.
    Regras:
    - Se usuário NÃO falar de período, aplicar este intervalo.
    - Se usuário falar de datas/meses/anos ("mês passado", "2025", etc.), ignorar este intervalo.
    """
    if not filters:
        return """
📌 Não há período padrão vindo do dashboard.
- Sempre infira o período a partir da pergunta do usuário.
"""

    data_inicio_txt = filters.get("data_inicio")
    data_fim_txt = filters.get("data_fim")
    vendedores = filters.get("vendedores") or "Todos"
    empresas = filters.get("empresas") or "Todas"
    tipo_visita = filters.get("tipo_visita") or "Visão Geral"

    periodo_bloco = ""
    if data_inicio_txt and data_fim_txt:
        try:
            di = dt.datetime.strptime(data_inicio_txt, "%d/%m/%Y").date()
            df = dt.datetime.strptime(data_fim_txt, "%d/%m/%Y").date()
            di_iso = di.isoformat()
            df_plus1_iso = (df + dt.timedelta(days=1)).isoformat()
        except Exception:
            di_iso = data_inicio_txt
            df_plus1_iso = data_fim_txt

        periodo_bloco = f"""
📌 CONTEXTO DE FILTROS DO DASHBOARD (usar como padrão quando o usuário NÃO especificar período):

- Período padrão: de '{di_iso}' (inclusive) até '{df_plus1_iso}' (exclusivo).
  Ao gerar SQL, quando a pergunta NÃO mencionar período, aplique:
    • Para tabelas contele.contele_os:
        o.created_at >= '{di_iso}' AND o.created_at < '{df_plus1_iso}'
    • Para views com os_created_at (ex.: vw_todas_os_respostas e vw_pendencias):
        os_created_at >= '{di_iso}' AND os_created_at < '{df_plus1_iso}'

- Vendedores selecionados no dashboard: {vendedores}
- Empresas selecionadas no dashboard: {empresas}
- Tipo de visita selecionado: {tipo_visita}

REGRAS DE PRIORIDADE DE PERÍODO:
1. Se a pergunta DO USUÁRIO CONTÉM datas explícitas, meses, anos ou expressões como
   'mês passado', 'este mês', 'ano de 2025', 'últimos 30 dias', etc:
   → Use APENAS o período descrito na pergunta e IGNORE o período padrão do dashboard.
2. Se a pergunta NÃO menciona período de tempo:
   → Aplique OBRIGATORIAMENTE o período padrão acima nos campos de data.
"""
    else:
        periodo_bloco = """
📌 Filtros do dashboard sem datas válidas.
- Infira período apenas a partir da pergunta ou use regras temporais gerais.
"""

    return periodo_bloco


def gerar_sql_com_ia(
    pergunta_usuario: str,
    context: str = "",
    filters: Optional[Dict[str, Any]] = None,
    max_retries: int = 3,
    backoff_base: float = 0.8,
    history: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Gera SQL a partir da pergunta, levando em conta:
    - Schema do Contele
    - Regras de contagem de visitas/OS
    - Filtros do dashboard como período padrão (quando o usuário não especifica datas)
    - Histórico de conversa (para entender contexto, não como filtro SQL)
    """
    if not client:
        return "-- Erro: OpenAI não configurada"

    schema_info = get_contele_schema_info()
    filtros_bloco = _montar_bloco_filtros(filters)

    regras_temporais = f"""
🗓 REGRAS TEMPORAIS GERAIS:
- Se perguntar "mês de 10": usar intervalo [{ANO_ATUAL}-10-01, {ANO_ATUAL}-11-01)
- "este mês": [{ANO_ATUAL}-{MES_ATUAL:02d}-01, primeiro dia do próximo mês)
- "mês passado": mês anterior ao atual
- Não usar ano 2023 ou 2024 sem menção explícita
"""

    historico_txt = _formatar_historico(history)

    system_prompt = f"""Você é {IA_CONFIG['nome']}, {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.
Converta perguntas em SQL PostgreSQL válido.

{schema_info}

{regras_temporais}

{filtros_bloco}

INSTRUÇÕES GERAIS:
- Usar views e tabelas corretas conforme regras (principal para detalhes de OS: vw_todas_os_respostas).
- PARA CONTAR VISITAS / OS:
  * Prefira SEMPRE contele.contele_os (1 linha = 1 OS) com COUNT(*).
  * Se usar contele.vw_todas_os_respostas para contagem de visitas/OS, use OBRIGATORIAMENTE COUNT(DISTINCT task_id).
- LIMIT obrigatório (<=1000).
- Texto -> ILIKE '%termo%'.
- Retornar SOMENTE SQL (sem markdown, sem explicação).
- Se a pergunta for ambígua ("Qual é o número dessa OS?") -> pegar última OS:
  SELECT os, assignee_name, poi, status, created_at
  FROM contele.contele_os
  ORDER BY created_at DESC
  LIMIT 1.
- O histórico de conversa serve APENAS para entender o contexto do que o usuário está perguntando
  (ex.: "e desse vendedor?", "e no mês passado?"), mas NUNCA como filtro automático de datas, vendedores ou clientes.
"""

    ultima_excecao = None

    for tentativa in range(1, max_retries + 1):
        try:
            user_content = f"Pergunta do usuário:\n{pergunta_usuario}"

            if historico_txt:
                user_content += (
                    "\n\nHistórico recente da conversa (somente contexto; não use como filtro SQL):\n"
                    f"{historico_txt}"
                )

            if context:
                user_content += (
                    "\n\nContexto de resumo de dados (apenas referência, não é para filtrar diretamente):\n"
                    f"{context[:2000]}"
                )

            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                temperature=0.1,
            )
            sql = response.choices[0].message.content.strip()
            sql = sql.replace("```sql", "").replace("```", "").strip()
            logger.info(
                json.dumps(
                    {
                        "etapa": "gerar_sql",
                        "tentativa": tentativa,
                        "pergunta": pergunta_usuario,
                        "sql_raw": sql,
                    },
                    ensure_ascii=False,
                )
            )
            return sql
        except Exception as e:
            ultima_excecao = e
            logger.error(f"Falha geração SQL tentativa {tentativa}: {e}")
            time.sleep(backoff_base * tentativa)

    return f"-- Erro ao gerar SQL após {max_retries} tentativas: {ultima_excecao}"

# ================== EXECUÇÃO DE SQL ==================


def executar_sql(sql: str) -> tuple:
    inicio = time.time()
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute(sql)
                if cur.description:
                    colunas = [desc.name for desc in cur.description]
                    linhas_dict = cur.fetchall()
                    linhas = [tuple(d.values()) for d in linhas_dict]
                    logger.info(
                        json.dumps(
                            {
                                "etapa": "executar_sql",
                                "sql": sql,
                                "linhas": len(linhas),
                                "duracao_ms": int(
                                    (time.time() - inicio) * 1000
                                ),
                            },
                            ensure_ascii=False,
                        )
                    )
                    return colunas, linhas
                return [], []
    except Exception as e:
        logger.error(
            json.dumps(
                {
                    "etapa": "erro_execucao_sql",
                    "sql": sql,
                    "erro": str(e),
                },
                ensure_ascii=False,
            )
        )
        raise Exception(f"Erro ao executar SQL: {e}")

# ================== ANÁLISE DOS RESULTADOS (CORRIGIDA + MEMÓRIA) ==================


def analisar_resultados_com_ia(
    pergunta_usuario: str,
    sql: str,
    colunas: list,
    linhas: list,
    history: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Usa métricas numéricas da query (COUNT, SUM, AVG, etc.),
    sem confundir len(linhas) com total de visitas/OS.
    Utiliza o histórico apenas para coerência narrativa.
    """
    if not client:
        return "Erro: OpenAI não configurada"

    metricas_numericas: Dict[str, Any] = {}
    if linhas:
        # caso típico: SELECT COUNT(*) AS total_visitas ...
        if len(linhas) == 1:
            row = linhas[0]
            for col_name, val in zip(colunas, row):
                if isinstance(val, (int, float)):
                    metricas_numericas[col_name] = val

    # Prévia de até 50 linhas para a IA enxergar o padrão
    preview_linhas = []
    for r in linhas[:50]:
        preview_linhas.append({col: val for col, val in zip(colunas, r)})

    resultado_estruturado = {
        "colunas": colunas,
        "total_linhas": len(linhas),
        "metricas_numericas": metricas_numericas,
        "preview_linhas": preview_linhas,
    }

    resultado_json = json.dumps(
        resultado_estruturado, ensure_ascii=False, default=str, indent=2
    )

    historico_txt = _formatar_historico(history)

    system_prompt = f"""Você é {IA_CONFIG['nome']}, {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.
Tom: {IA_CONFIG['tom']}. 

REGRAS IMPORTANTES (NÚMEROS):
- Use SEMPRE os valores de `metricas_numericas` como base principal para contagens, somas e médias.
- `total_linhas` é apenas o número de linhas retornadas pela query, NÃO é o total de OS, visitas, etc.
- Se existir `metricas_numericas.total_os`, `metricas_numericas.total_visitas`, etc., esses são os números principais.
- Quando não houver métricas numéricas, descreva o padrão das linhas do preview.
- Use o histórico recente da conversa apenas para manter coerência na explicação, sem inventar números.

TRATAMENTO ESPECIAL DE OBJETIVOS / ABORDAGENS:
- Se os resultados tiverem colunas como 'objetivo' ou 'objetivo_legenda' junto com uma métrica (ex.: total_os, total, qtd_visitas, etc.):
  • Considere cada valor distinto de 'objetivo'/'objetivo_legenda' como UM tipo de visita.
  • Se aparecer algo como 'Abordagem sem sucesso', trate como um tipo específico de visita
    (tentativa que não evoluiu), NÃO como erro de categorização.
  • Quando houver linha em que o objetivo esteja nulo, vazio ou rótulos do tipo:
        - NULL
        - ''
        - 'sem objetivo'
        - 'sem objetivo informado'
    interprete isso assim:
        → Em contexto de visitas (vw_visitas_status, visitas por objetivo, etc.), 
          descreva como "Abordagens sem sucesso ou visitas sem objetivo definido",
          e NÃO trate automaticamente como falha de processo.
    Você pode falar algo como:
        "Além disso, há 4 abordagens sem sucesso/sem objetivo definido."

- NÃO fique repetindo que isso é "erro de categorização" ou "ponto de atenção" por padrão.
  Só faça esse tipo de comentário se:
  • a própria pergunta do usuário for sobre qualidade dos registros, problemas de preenchimento,
    campos obrigatórios, categorização etc.
  • OU se a quantidade de registros sem objetivo for claramente relevante (por exemplo, muito alta).

RESUMINDO:
- Categoria "Abordagem sem sucesso" = métrica de tentativa de visita que não deu certo.
- Categoria "Sem objetivo informado" = trate de forma neutra; você pode mencionar o número,
  mas NÃO assuma automaticamente que é erro.

Formate a resposta em:
1. 📊 Resumo direto, com números explícitos (ex: "Relacionamento: 204 OS, Prospecção: 59, Abordagens sem sucesso: 4").
2. 🔍 Principais insights (máx. 5).
3. 💡 Recomendações objetivas (se fizer sentido).
"""

    try:
        user_content_parts = [
            f"Pergunta original do usuário:\n{pergunta_usuario}\n",
        ]
        if historico_txt:
            user_content_parts.append(
                "Histórico recente da conversa (use apenas para coerência da narrativa, não para alterar os números):\n"
                f"{historico_txt}\n"
            )
        user_content_parts.append(f"SQL executado:\n{sql}\n")
        user_content_parts.append(f"Resultados estruturados (JSON):\n{resultado_json}\n")
        user_content_parts.append("Faça a análise seguindo as regras.")

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "\n".join(user_content_parts)},
            ],
            temperature=0.6,
        )
        texto = response.choices[0].message.content.strip()
        logger.info(
            json.dumps(
                {
                    "etapa": "analisar_resultados",
                    "pergunta": pergunta_usuario,
                    "linhas": len(linhas),
                    "metricas": metricas_numericas,
                },
                ensure_ascii=False,
            )
        )
        return texto
    except Exception as e:
        logger.error(f"Erro análise IA: {e}")
        return f"Erro ao analisar: {e}"

# ================== MODOS DE RESPOSTA ==================


def conversar_casualmente(
    pergunta: str,
    history: Optional[List[Dict[str, Any]]] = None,
) -> str:
    if not client:
        return "❌ OpenAI não configurada"

    historico_txt = _formatar_historico(history)

    system_prompt = f"""Você é {IA_CONFIG['nome']}, {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.
Tom: {IA_CONFIG['tom']}
Especialidade: {IA_CONFIG['especialidade']}
Conversa casual com João. Não mencione banco/SQL espontaneamente.
Use o histórico recente apenas para manter o fio da conversa.
"""

    if historico_txt:
        user_content = (
            "Histórico recente da conversa:\n"
            f"{historico_txt}\n\n"
            f"Mensagem atual do usuário:\n{pergunta}"
        )
    else:
        user_content = pergunta

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            temperature=0.9,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Erro conversa casual: {e}")
        return f"❌ Erro: {e}"


def responder_pergunta_livre(
    pergunta: str,
    context: str = "",
    filters: Optional[Dict[str, Any]] = None,
    history: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Entrada principal usada pelo Streamlit.

    - `context`: resumo textual dos dados atuais (opcional, vindo do dashboard)
    - `filters`: dicionário com filtros ativos (período, vendedores, empresas, tipo_visita)
      usado como período padrão quando a pergunta não especifica tempo.
    - `history`: lista de mensagens [{role: "user"/"assistant", text: "..."}]
      para manter memória da conversa.
    """
    if filters is None:
        filters = {}

    tipo = detectar_tipo_pergunta(pergunta)

    if tipo == "casual":
        return conversar_casualmente(pergunta, history=history)

    if tipo == "meta":
        return f"""**Olá, João! Eu sou {IA_CONFIG['nome']} 👋**
🎯 Papel: {IA_CONFIG['papel']} na {IA_CONFIG['empresa']}
💼 Especialidade: {IA_CONFIG['especialidade']}
🎨 Estilo: {IA_CONFIG['tom']}
🔧 Capacidades:
- Analiso OS's, clientes, vendedores e objetivos
- Gero e valido SQL (somente leitura)
- Traço rankings, timelines e pendências
- Resumos detalhados de visitas (vw_todas_os_respostas)
🛡 Blindagem ativa: validação, retry, logs e controle temporal
💡 Exemplos:
- Quantas OS por objetivo?
- Resumo da OS 5078
- Pendências abertas
- Ranking de vendedores
- Clientes com mais visitas
🚀 Pronto para sua pergunta sobre dados!"""

    try:
        sql_bruto = gerar_sql_com_ia(
            pergunta,
            context=context,
            filters=filters,
            history=history,
        )
        if sql_bruto.startswith("-- Erro"):
            return f"❌ {sql_bruto}"

        valido, sql_validado = validar_e_corrigir_sql(sql_bruto)
        if not valido:
            if "genérica" in sql_validado.lower() or "genérica" in sql_validado:
                return (
                    f"{sql_validado}\n🔁 DICA: Especifique OS, período ou objetivo."
                )
            return sql_validado

        colunas, linhas = executar_sql(sql_validado)

        if not linhas:
            return (
                f"❌ Nenhum resultado encontrado.\nQuery:\n```sql\n{sql_validado}\n```"
            )

        analise = analisar_resultados_com_ia(
            pergunta, sql_validado, colunas, linhas, history=history
        )
        return (
            f"{analise}\n\n---\n"
            f"**📌 Query executada:**\n```sql\n{sql_validado}\n```\n"
            f"**📊 Linhas retornadas:** {len(linhas)}"
        )
    except Exception as e:
        logger.error(f"Erro pipeline dados: {e}")
        return f"❌ Erro: {str(e)}"

# ================== CHECAGENS / STATUS ==================


def ia_disponivel() -> Tuple[bool, str]:
    if not OPENAI_API_KEY:
        return False, "❌ Chave OpenAI não configurada"
    if not DATABASE_URL:
        return False, "❌ DATABASE_URL não configurado"
    if not client:
        return False, "❌ Erro ao inicializar OpenAI"
    return True, f"✅ {IA_CONFIG['nome']} disponível - {IA_CONFIG['papel']} 🛡 Blindagem ativa"


def testar_openai() -> str:
    if not client:
        return "❌ Client não inicializado"
    try:
        client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "ping"},
                {"role": "user", "content": "ping"},
            ],
            temperature=0,
        )
        return "✅ OpenAI OK"
    except Exception as e:
        return f"❌ OpenAI falhou: {e}"


def testar_banco() -> str:
    if not DATABASE_URL:
        return "❌ DATABASE_URL ausente"
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return "✅ Banco OK"
    except Exception as e:
        return f"❌ Banco falhou: {e}"


def testar_integridade() -> str:
    resultados = {
        "openai": testar_openai(),
        "banco": testar_banco(),
        "ano_atual": ANO_ATUAL,
        "mes_atual": MES_ATUAL,
    }
    return json.dumps(resultados, ensure_ascii=False, indent=2)

# ================== CLI ==================


def chat():
    print(f"\n{'='*70}")
    print(f"💬 Chat com {IA_CONFIG['nome']} - {IA_CONFIG['papel']}")
    print(f"{'='*70}\n")
    print(f"👋 Olá, João! Eu sou {IA_CONFIG['nome']}.")
    print("\nExemplos:")
    print(" • Resumo da OS 5078")
    print(" • Pendências abertas")
    print(" • Ranking de vendedores por visitas")
    print(" • Clientes com mais visitas este mês")
    print(" • Quantas OS por objetivo?")
    print(" • Última OS registrada")
    print(f"\n🛡 Blindagem ativa | 🗓 Ano: {ANO_ATUAL} Mês: {MES_ATUAL}")
    print("Digite 'teste' para auto-diagnóstico, 'sair' para encerrar.\n" + "-" * 70)

    history: List[Dict[str, str]] = []

    while True:
        pergunta = input("Você: ").strip()
        if pergunta.lower() in ["sair", "exit", "quit", "tchau"]:
            print(f"\n{IA_CONFIG['nome']}: Até logo, João! 👋\n")
            break
        if pergunta.lower() == "teste":
            print(f"\n{testar_integridade()}\n" + "-" * 70)
            continue
        if not pergunta:
            continue

        history.append({"role": "user", "text": pergunta})
        resposta = responder_pergunta_livre(pergunta, history=history)
        print(f"\n{IA_CONFIG['nome']}:\n{resposta}\n")
        print("-" * 70 + "\n")
        history.append({"role": "assistant", "text": resposta})


if __name__ == "__main__":
    ok, msg = ia_disponivel()
    if not ok:
        print(msg)
    else:
        chat()