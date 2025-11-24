#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Agente de IA para análise do banco Contele
John - Analista de Dados Sênior da TecnoTop Automação
"""
import os
import psycopg2
import psycopg2.extras
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Configurações
DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# 🎭 PERSONALIZAÇÃO DA IA
IA_CONFIG = {
    "nome": "John",
    "papel": "Analista de Dados Sênior",
    "tom": "Profissional com senso de humor aguçado, refletindo o estilo do usuário João sem perder objetividade",
    "especialidade": "análise de visitas técnicas e relacionamento com clientes B2B nos setores industrial e comercial. Especialista em produtos/marcas (Festo, Wago, Hengst, Rexroth) com foco no mercado da Região do Rio de Janeiro",
    "empresa": "TecnoTop Automação"
}

def get_contele_schema_info() -> str:
    """Retorna descrição COMPLETA e ATUALIZADA do schema para a IA"""
    return f"""
# 📊 ESTRUTURA DO BANCO DE DADOS CONTELE - VERSÃO COMPLETA

## 🎯 CONTEXTO DO NEGÓCIO
- Sistema de Field Service da {IA_CONFIG['empresa']}
- Rastreia visitas técnicas (OS's) realizadas por técnicos/vendedores em clientes (POI's)
- Cada visita tem formulários com perguntas e respostas sobre diferentes objetivos
- Região de atuação: Sul Fluminense/RJ
- Segmentos: Industrial e Comercial
- Principais marcas: Festo, Wago, Hengst, Rexroth

## 📋 TABELAS PRINCIPAIS - COLUNAS EXATAS

### contele.contele_os (OS's com objetivo definido)
Colunas: task_id, os, poi, title, status, assignee_name, assignee_id, created_at, finished_at, updated_at, ingested_at, updated_at_local

### contele.contele_os_all (TODAS as OS's - com e sem objetivo)
Colunas: task_id, os, poi, title, status, assignee_name, assignee_id, created_at, finished_at, updated_at, ingested_at, updated_at_local, has_objetivo

### contele.contele_answers (Respostas dos formulários - apenas com objetivo)
Colunas: task_id, os, poi, form_title, question_id, question_title, answer_human, answer_raw, created_at, ingested_at

### contele.contele_answers_all (TODAS as respostas)
Colunas: task_id, os, poi, form_title, question_id, question_title, answer_human, answer_raw, created_at, ingested_at

## 🔍 VIEWS ANALÍTICAS - COLUNAS EXATAS

### contele.vw_todas_os_respostas (View normalizada principal)
Colunas: task_id, os, poi, form_title, question_title, answer_human, created_at, assignee_name, status, os_created_at, os_finished_at
⭐ USE ESTA para análises de respostas com informação do vendedor/técnico

### contele.vw_prospeccao (Pivotada - Objetivo: Prospecção)
Colunas FIXAS: task_id, os, poi, assignee_name, status, os_created_at, os_finished_at
Colunas DINÂMICAS: perguntas específicas de prospecção como colunas
⭐ AGORA TEM assignee_name e status!

### contele.vw_relacionamento (Pivotada - Objetivo: Relacionamento)
Colunas FIXAS: task_id, os, poi, assignee_name, status, os_created_at, os_finished_at
Colunas DINÂMICAS: perguntas específicas de relacionamento como colunas
⭐ AGORA TEM assignee_name e status!

### contele.vw_levantamento_de_necessidade (Pivotada - Objetivo: Levantamento)
Colunas FIXAS: task_id, os, poi, assignee_name, status, os_created_at, os_finished_at
Colunas DINÂMICAS: perguntas específicas de levantamento como colunas
⭐ AGORA TEM assignee_name e status!

### contele.vw_visita_tecnica (Pivotada - Objetivo: Visita Técnica)
Colunas FIXAS: task_id, os, poi, assignee_name, status, os_created_at, os_finished_at
Colunas DINÂMICAS: perguntas específicas de visita técnica como colunas
⭐ AGORA TEM assignee_name e status!

## 📊 VIEWS DE RESUMO (NOVAS!)

### contele.vw_resumo_vendedores (Estatísticas por vendedor/técnico)
Colunas: assignee_name, total_os, total_clientes, os_concluidas, os_pendentes, primeira_visita, ultima_visita, total_prospeccao, total_relacionamento, total_levantamento, total_visita_tecnica
⭐ USE ESTA para análises rápidas de desempenho de vendedores!

### contele.vw_resumo_clientes (Estatísticas por cliente/POI)
Colunas: poi, total_visitas, total_vendedores_distintos, primeira_visita, ultima_visita, vendedores (array), visitas_prospeccao, visitas_relacionamento, visitas_levantamento, visitas_tecnicas
⭐ USE ESTA para análises rápidas de clientes!

### contele.vw_timeline_atividades (Timeline mensal - últimos 6 meses)
Colunas: mes, assignee_name, total_visitas, clientes_visitados, visitas_concluidas
⭐ USE ESTA para análises temporais/tendências!

## 💡 REGRAS PARA SQL - MUITO IMPORTANTE!

1. **Para análises de vendedores/técnicos:**
   - Ranking/Top: USE contele.vw_resumo_vendedores
   - Detalhes de OS's: USE contele.contele_os
   - Tem: assignee_name, status, datas

2. **Para análises de clientes:**
   - Ranking/Top: USE contele.vw_resumo_clientes
   - Detalhes: USE contele.contele_os com GROUP BY poi

3. **Para análises de respostas:**
   - USE: contele.vw_todas_os_respostas
   - Tem: question_title, answer_human, assignee_name

4. **Para análises por objetivo específico:**
   - Prospecção: USE contele.vw_prospeccao
   - Relacionamento: USE contele.vw_relacionamento
   - Levantamento: USE contele.vw_levantamento_de_necessidade
   - Visita Técnica: USE contele.vw_visita_tecnica
   - TODAS têm assignee_name agora!

5. **Para análises temporais:**
   - USE: contele.vw_timeline_atividades (últimos 6 meses)
   - OU: contele.contele_os com DATE_TRUNC

6. **SEMPRE use LIMIT (máximo 1000)**

7. **Para buscar texto use ILIKE '%termo%'**

8. **NUNCA use MAX(CASE...) ou COUNT(CASE...) dentro de GROUP BY**

## 📌 EXEMPLOS CORRETOS - ATUALIZADOS

### Ranking de vendedores (RÁPIDO):
SELECT assignee_name, total_os, total_clientes, os_concluidas
FROM contele.vw_resumo_vendedores
ORDER BY total_os DESC
LIMIT 20;

### Top 10 clientes (RÁPIDO):
SELECT poi, total_visitas, vendedores, primeira_visita, ultima_visita
FROM contele.vw_resumo_clientes
ORDER BY total_visitas DESC
LIMIT 10;

### Timeline de atividades:
SELECT mes, assignee_name, total_visitas, clientes_visitados
FROM contele.vw_timeline_atividades
ORDER BY mes DESC, total_visitas DESC
LIMIT 100;

### Contar OS's por objetivo:
SELECT answer_human as objetivo, COUNT(DISTINCT task_id) as total
FROM contele.vw_todas_os_respostas
WHERE question_title ILIKE 'Qual objetivo%'
GROUP BY answer_human
ORDER BY total DESC
LIMIT 100;

### OS's de prospecção com vendedor:
SELECT task_id, os, poi, assignee_name, status, os_created_at
FROM contele.vw_prospeccao
WHERE assignee_name IS NOT NULL
LIMIT 100;

### Buscar respostas específicas:
SELECT task_id, os, poi, assignee_name, question_title, answer_human
FROM contele.vw_todas_os_respostas
WHERE answer_human ILIKE '%termo%'
LIMIT 100;

### OS's por status:
SELECT status, COUNT(*) as total
FROM contele.contele_os
GROUP BY status
ORDER BY total DESC
LIMIT 50;

### Clientes visitados por um vendedor:
SELECT poi, COUNT(*) as visitas
FROM contele.contele_os
WHERE assignee_name ILIKE '%nome%'
GROUP BY poi
ORDER BY visitas DESC
LIMIT 50;
"""

def detectar_tipo_pergunta(pergunta: str) -> str:
    """
    Detecta se é uma conversa casual, meta-pergunta ou pergunta sobre dados
    Retorna: 'casual', 'meta', 'dados'
    """
    pergunta_lower = pergunta.lower().strip()
    
    # Conversas casuais (cumprimentos, agradecimentos, despedidas)
    conversas_casuais = [
        "oi", "olá", "ola", "hey", "hi", "hello", 
        "bom dia", "boa tarde", "boa noite", "bom diaa",
        "tudo bem", "como vai", "como está", "beleza", "e aí", "eai",
        "obrigado", "obrigada", "valeu", "vlw", "brigadão", "brigado",
        "tchau", "até logo", "falou", "até mais", "flw",
        "legal", "bacana", "show", "top", "massa", "dahora"
    ]
    
    # Meta-perguntas (sobre a própria IA)
    meta_keywords = [
        "quem é você", "quem você é", "quem voce é", "quem voce e",
        "o que você faz", "o que voce faz", "qual seu objetivo",
        "para que serve", "sua função", "sua individualidade", 
        "se apresente", "seu papel", "sua especialidade", 
        "quem és", "qual é seu nome", "qual e seu nome",
        "o que você consegue", "suas capacidades", 
        "que tipo de pergunta", "pode me ajudar", "consegue",
        "ajuda", "help", "como funciona"
    ]
    
    # Palavras-chave que indicam perguntas sobre DADOS
    dados_keywords = [
        "quantas", "quantos", "quanto", "total", "soma", "média", "media",
        "mostre", "liste", "exiba", "busque", "encontre", "procure",
        "os's", "visita", "cliente", "vendedor", "técnico", "tecnico",
        "poi", "task", "objetivo", "prospecção", "prospeccao",
        "relacionamento", "levantamento", "ranking", "top",
        "último", "ultima", "mês", "mes", "ano", "período", "periodo",
        "status", "concluída", "concluida", "pendente", "finalizada",
        "comparar", "comparação", "comparacao", "diferença", "diferenca"
    ]
    
    # Verifica conversas casuais primeiro (mais específicas)
    if any(casual == pergunta_lower or pergunta_lower.startswith(casual) for casual in conversas_casuais):
        return 'casual'
    
    # Verifica meta-perguntas
    if any(meta in pergunta_lower for meta in meta_keywords):
        return 'meta'
    
    # Verifica perguntas sobre dados
    if any(dado in pergunta_lower for dado in dados_keywords):
        return 'dados'
    
    # Se não detectou nada específico, assume que é pergunta sobre dados
    # (para não bloquear perguntas válidas)
    return 'dados'

def gerar_sql_com_ia(pergunta_usuario: str) -> str:
    """Gera SQL baseado na pergunta do usuário"""
    if not client:
        return "-- Erro: OpenAI não configurada"
    
    schema_info = get_contele_schema_info()
    
    system_prompt = f"""Você é {IA_CONFIG['nome']}, um {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.

Sua tarefa é converter perguntas em português para queries SQL PostgreSQL VÁLIDAS e OTIMIZADAS.

{schema_info}

INSTRUÇÕES CRÍTICAS:
- Use APENAS as tabelas e views listadas acima
- PRIORIZE views de resumo (vw_resumo_vendedores, vw_resumo_clientes) quando aplicável
- Sempre adicione LIMIT (máximo 1000)
- Retorne APENAS o SQL válido, sem explicações ou markdown
- Para buscar texto, use ILIKE '%termo%'
- NUNCA use funções de agregação (COUNT, MAX, etc) dentro de GROUP BY
- Para contar por objetivo: WHERE question_title ILIKE 'Qual objetivo%' GROUP BY answer_human
- LEMBRE-SE: views pivotadas AGORA têm assignee_name, status, os_created_at, os_finished_at
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": pergunta_usuario}
            ],
            temperature=0.1,
        )
        sql = response.choices[0].message.content.strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()
        return sql
    except Exception as e:
        return f"-- Erro ao gerar SQL: {e}"

def executar_sql(sql: str) -> tuple:
    """Executa SQL e retorna (colunas, linhas)"""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                if cur.description:
                    colunas = [desc[0] for desc in cur.description]
                    linhas = cur.fetchall()
                    return colunas, linhas
                return [], []
    except Exception as e:
        raise Exception(f"Erro ao executar SQL: {e}")

def analisar_resultados_com_ia(pergunta_usuario: str, sql: str, colunas: list, linhas: list) -> str:
    """IA analisa os resultados e responde em linguagem natural"""
    if not client:
        return "Erro: OpenAI não configurada"
    
    linhas_preview = linhas[:100]
    resultado_texto = f"Colunas: {', '.join(colunas)}\nTotal: {len(linhas)}\n\nPrimeiras linhas:\n"
    for linha in linhas_preview:
        resultado_texto += f"{linha}\n"
    
    system_prompt = f"""Você é {IA_CONFIG['nome']}, um {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.
Seu tom é {IA_CONFIG['tom']}.

Analise os resultados SQL e responda de forma clara e objetiva.

FORMATO DA RESPOSTA:
1. **📊 Resumo:** Resposta direta à pergunta (1-2 frases)
2. **🔍 Principais Insights:** 3-5 pontos principais dos dados
3. **💡 Recomendações:** Sugestões práticas baseadas nos dados (quando aplicável)

Use emojis moderadamente e mantenha tom profissional com toques de humor.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Pergunta: {pergunta_usuario}\n\nSQL:\n{sql}\n\nResultados:\n{resultado_texto}\n\nAnalise:"}
            ],
            temperature=0.7,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Erro ao analisar: {e}"

def conversar_casualmente(pergunta: str) -> str:
    """Responde conversas casuais sem acessar o banco"""
    if not client:
        return "❌ OpenAI não configurada"
    
    system_prompt = f"""Você é {IA_CONFIG['nome']}, um {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}.
Seu tom é {IA_CONFIG['tom']}.
Sua especialidade: {IA_CONFIG['especialidade']}

Você está em uma conversa casual com João (seu usuário).
Seja amigável, breve e natural. Mantenha tom profissional mas descontraído.
NÃO mencione SQL ou banco de dados a menos que João pergunte especificamente.
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": pergunta}
            ],
            temperature=0.8,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"❌ Erro: {e}"

def responder_pergunta_livre(pergunta: str, context: str = "", filters: dict = None) -> str:
    """Responde perguntas de forma inteligente: casual, meta ou dados"""
    if filters is None:
        filters = {}
    
    tipo = detectar_tipo_pergunta(pergunta)
    
    # Conversa casual (cumprimentos, agradecimentos, etc)
    if tipo == 'casual':
        return conversar_casualmente(pergunta)
    
    # Meta-perguntas (sobre a própria IA)
    if tipo == 'meta':
        return f"""**Olá, João! Eu sou {IA_CONFIG['nome']} 👋**

**🎯 Meu Papel:** {IA_CONFIG['papel']} da {IA_CONFIG['empresa']}

**💼 Minha Especialidade:** {IA_CONFIG['especialidade']}

**🎨 Meu Estilo:** {IA_CONFIG['tom']}

**🔧 O que eu faço:**
- ✅ Analiso dados de visitas técnicas e OS's do sistema Contele
- ✅ Gero relatórios e insights sobre clientes, vendedores/técnicos e objetivos
- ✅ Respondo perguntas em linguagem natural sobre os dados
- ✅ Crio queries SQL automaticamente e otimizadas
- ✅ Forneço recomendações estratégicas baseadas em dados

**🆕 Novidades (views de resumo rápido!):**
- Rankings de vendedores por desempenho
- Top clientes com histórico completo
- Timeline de atividades mensais

**💡 Exemplos de perguntas:**
- "Quantas OS's temos por objetivo?"
- "Quais os top 10 clientes com mais visitas?"
- "Qual vendedor/técnico tem mais visitas?"
- "Mostre OS's de prospecção do último mês"
- "Timeline de atividades dos últimos meses"
- "Clientes que foram visitados por mais de um vendedor"

Estou aqui para tornar a análise de dados simples, rápida e eficiente! 🚀"""
    
    # Perguntas sobre DADOS (gera SQL)
    try:
        sql = gerar_sql_com_ia(pergunta)
        
        if sql.startswith("--"):
            return f"❌ {sql}"
        
        colunas, linhas = executar_sql(sql)
        
        if not linhas:
            return "❌ Nenhum resultado encontrado para esta consulta."
        
        analise = analisar_resultados_com_ia(pergunta, sql, colunas, linhas)
        
        return f"{analise}\n\n---\n**📌 Query executada:**\n```sql\n{sql}\n```\n**📊 Linhas retornadas:** {len(linhas)}"
    
    except Exception as e:
        return f"❌ Erro: {str(e)}"

def ia_disponivel() -> tuple:
    """Verifica se a IA está disponível"""
    if not OPENAI_API_KEY:
        return False, "❌ Chave OpenAI não configurada"
    if not DATABASE_URL:
        return False, "❌ DATABASE_URL não configurado"
    if not client:
        return False, "❌ Erro ao inicializar OpenAI"
    
    return True, f"✅ {IA_CONFIG['nome']} disponível - {IA_CONFIG['papel']}"

def chat():
    """Interface de chat com a IA"""
    print(f"\n{'='*70}")
    print(f"💬 Chat com {IA_CONFIG['nome']} - {IA_CONFIG['papel']}")
    print(f"{'='*70}\n")
    print(f"👋 Olá, João! Eu sou {IA_CONFIG['nome']}, seu {IA_CONFIG['papel']} na {IA_CONFIG['empresa']}.")
    print(f"\nPosso te ajudar a analisar dados de visitas técnicas, OS's e clientes.\n")
    print("💡 Exemplos:")
    print("   • Quantas OS's temos por objetivo?")
    print("   • Quais os top 10 clientes?")
    print("   • Qual vendedor/técnico tem mais visitas?")
    print("   • Timeline de atividades dos últimos meses")
    print(f"\nDigite 'sair' para encerrar.\n{'-'*70}\n")
    
    while True:
        pergunta = input("Você: ").strip()
        
        if pergunta.lower() in ['sair', 'exit', 'quit', 'tchau']:
            print(f"\n{IA_CONFIG['nome']}: Até logo, João! 👋\n")
            break
        
        if not pergunta:
            continue
        
        try:
            resposta = responder_pergunta_livre(pergunta)
            print(f"\n{IA_CONFIG['nome']}:\n{resposta}\n")
            print("-" * 70 + "\n")
            
        except Exception as e:
            print(f"❌ Erro: {e}\n")
            print("-" * 70 + "\n")

if __name__ == "__main__":
    if not DATABASE_URL:
        print("❌ Configure DATABASE_URL no .env")
    elif not OPENAI_API_KEY:
        print("❌ Configure OPENAI_API_KEY no .env")
    else:
        chat()