import os
from datetime import datetime, timedelta
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

st.set_page_config(page_title="TecnoTop | Contele Forms", layout="wide")

# Custom CSS para sidebar mais profissional
st.markdown("""
<style>
    [data-testid="stSidebar"] {
        background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
    }
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2 {
        color: #ffffff;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    [data-testid="stSidebar"] label {
        color: #e0e0e0;
        font-weight: 600;
        font-size: 0.95rem;
    }
    [data-testid="stSidebar"] hr {
        border-color: #ffffff33;
        margin: 1.5rem 0;
    }
    [data-testid="stSidebar"] p {
        color: #b0b0b0;
        font-size: 0.85rem;
        line-height: 1.4;
    }
</style>
""", unsafe_allow_html=True)

DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4-mini")

# Configurar OpenAI (novo formato)
client = OpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

# ---------- helpers ----------
@st.cache_resource(show_spinner=False)
def get_engine():
    if not DATABASE_URL:
        raise RuntimeError("Defina DATABASE_URL no .env")
    return create_engine(DATABASE_URL, pool_pre_ping=True)

@st.cache_data(show_spinner=False, ttl=60)
def fetch(sql: str):
    with get_engine().connect() as c:
        return pd.read_sql(text(sql), c)

@st.cache_data(ttl=300)
def column_exists(table_schema: str, table_name: str, column_name: str) -> bool:
    try:
        sql = f"""
          select 1 from information_schema.columns
          where table_schema = '{table_schema}'
            and table_name = '{table_name}'
            and column_name = '{column_name}'
          limit 1
        """
        return not fetch(sql).empty
    except Exception:
        return False

def get_ia_response(user_message: str, context: str = "") -> str:
    """Chama OpenAI (v1.0+) para gerar respostas contextualizadas sobre os dados."""
    if not client:
        return "⚠️ Chave OpenAI não configurada. Defina OPENAI_API_KEY no .env"
    
    try:
        system_prompt = f"""Você é um assistente de análise de dados para um dashboard de formulários de vendas da Contele.
Sua função é ajudar a interpretar métricas, identificar tendências e sugerir ações.

CONTEXTO DOS DADOS ATUAIS:
{context}

INSTRUÇÕES:
- Responda de forma concisa em português brasileiro
- Forneça insights acionáveis e específicos
- Use os números do contexto para embasar suas análises
- Sugerir melhorias e oportunidades quando relevante
- Se não tiver informação, seja honesto

Você tem acesso ao schema 'contele' com as seguintes tabelas principais:
- contele_os: Ordens de serviço com informações de vendedor (assignee_name) e POI (empresa)
- contele_answers: Respostas dos formulários com perguntas e respostas
- vw_prospeccao, vw_relacionamento, vw_levantamento_de_necessidade, vw_visita_tecnica: Views filtradas"""
        
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ],
            temperature=0.7,
            max_tokens=800
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"❌ Erro ao chamar IA: {str(e)}"

def build_context_summary(where_clause: str, TASKS_VIEW: str) -> str:
    """Constrói um resumo completo dos dados para contexto da IA."""
    try:
        # Resumo principal
        summary_data = fetch(f"""
            select 
              count(distinct o.task_id) as total_formularios,
              count(distinct o.assignee_name) as total_vendedores,
              count(distinct o.poi) as total_empresas,
              max(a.created_at) as ultima_atualizacao,
              min(a.created_at) as primeira_atualizacao
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause}
        """)
        
        # Top 5 vendedores
        top_vendedores = fetch(f"""
            select o.assignee_name, count(distinct o.task_id) as total
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause}
            group by o.assignee_name
            order by total desc
            limit 5
        """)
        
        # Top 5 POIs
        top_pois = fetch(f"""
            select o.poi, count(distinct o.task_id) as total
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause}
            group by o.poi
            order by total desc
            limit 5
        """)
        
        # Distribuição por tipo
        tipos = fetch(f"""
            select case 
                     when a.form_title ilike '%prospec%' then 'Prospecção'
                     when a.form_title ilike '%relat%' then 'Relacionamento'
                     when a.form_title ilike '%levant%' then 'Levantamento'
                     when a.form_title ilike '%visita%' then 'Visita Técnica'
                     else 'Outro'
                   end as tipo,
                   count(distinct o.task_id) as total
            from contele.contele_answers a
            join contele.contele_os o using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause}
            group by tipo
        """)
        
        context = "=== RESUMO DOS DADOS ===\n"
        
        if not summary_data.empty:
            row = summary_data.iloc[0]
            context += f"""
PERÍODO: {row['primeira_atualizacao']} até {row['ultima_atualizacao']}
- Total de Formulários: {int(row['total_formularios'])}
- Vendedores Ativos: {int(row['total_vendedores'])}
- Empresas Visitadas: {int(row['total_empresas'])}

"""
        
        if not top_vendedores.empty:
            context += "TOP 5 VENDEDORES:\n"
            for idx, row in top_vendedores.iterrows():
                context += f"  {idx+1}. {row['assignee_name']}: {int(row['total'])} formulários\n"
            context += "\n"
        
        if not top_pois.empty:
            context += "TOP 5 EMPRESAS (POIs):\n"
            for idx, row in top_pois.iterrows():
                context += f"  {idx+1}. {row['poi']}: {int(row['total'])} visitas\n"
            context += "\n"
        
        if not tipos.empty:
            context += "DISTRIBUIÇÃO POR TIPO:\n"
            for idx, row in tipos.iterrows():
                context += f"  {row['tipo']}: {int(row['total'])} visitas\n"
        
        return context
    except Exception as e:
        return f"Erro ao buscar contexto: {str(e)}"

# ========== FILTROS (SIDEBAR) ==========
with st.sidebar:
    st.header("🔍 Filtros")
    
    st.subheader("📅 Período")
    data_inicio = st.date_input(
        "Data Início",
        value=datetime.now() - timedelta(days=90),
        format="DD/MM/YYYY",
        key="f_data_inicio",
    )
    data_fim = st.date_input(
        "Data Fim",
        value=datetime.now(),
        format="DD/MM/YYYY",
        key="f_data_fim",
    )

    st.divider()
    st.subheader("👥 Equipe")
    
    try:
        vendedores_list = fetch("select distinct assignee_name from contele.contele_os where assignee_name is not null order by assignee_name")
        vendedores_options = ["Todos"] + vendedores_list['assignee_name'].tolist()
    except Exception:
        vendedores_options = ["Todos"]
    vendedor_selecionado = st.multiselect(
        "Vendedor(es)",
        vendedores_options,
        default=["Todos"],
        key="f_vendedor",
    )

    st.divider()
    st.subheader("🏢 Dados")
    
    try:
        empresas_list = fetch("select distinct poi from contele.contele_os where poi is not null order by poi")
        empresas_options = ["Todas"] + empresas_list['poi'].tolist()
    except Exception:
        empresas_options = ["Todas"]
    empresa_selecionada = st.multiselect(
        "Empresa(s)",
        empresas_options,
        default=["Todas"],
        key="f_empresa",
    )

    objetivo = st.selectbox(
        "Tipo de Visita",
        ["Visão Geral", "Prospecção", "Relacionamento", "Levantamento de Necessidade", "Visita Técnica"],
        index=0,
        key="f_objetivo",
    )

    st.divider()
    st.caption("✨ Filtros aplicam-se a todas as análises desta página.")

# ========== BUILD WHERE CLAUSE ==========
def build_where_clause():
    conditions = []
    conditions.append(f"a.created_at >= '{data_inicio.isoformat()}'")
    conditions.append(f"a.created_at <= '{data_fim.isoformat()} 23:59:59'")

    if "Todos" not in vendedor_selecionado:
        vendedores_str = "', '".join([v.replace("'", "''") for v in vendedor_selecionado])
        conditions.append(f"o.assignee_name IN ('{vendedores_str}')")

    if "Todas" not in empresa_selecionada:
        empresas_str = "', '".join([e.replace("'", "''") for e in empresa_selecionada])
        conditions.append(f"o.poi IN ('{empresas_str}')")

    return " AND ".join(conditions) if conditions else "1=1"

where_clause = build_where_clause()

# ========== TASKS VIEW ==========
COMBINED_VIEWS = """
  (select task_id from contele.vw_prospeccao
   union
   select task_id from contele.vw_relacionamento
   union
   select task_id from contele.vw_levantamento_de_necessidade
   union
   select task_id from contele.vw_visita_tecnica)
"""

view_map = {
    "Prospecção": "vw_prospeccao",
    "Relacionamento": "vw_relacionamento",
    "Levantamento de Necessidade": "vw_levantamento_de_necessidade",
    "Visita Técnica": "vw_visita_tecnica",
}

if objetivo == "Visão Geral":
    TASKS_VIEW = COMBINED_VIEWS
else:
    vw_name = view_map.get(objetivo)
    TASKS_VIEW = f"(select task_id from contele.{vw_name})" if vw_name else COMBINED_VIEWS

# Buscar contexto dos dados (uma vez por sessão)
context = build_context_summary(where_clause, TASKS_VIEW)

# ========== ESTRUTURA DE ABAS PRINCIPAIS ==========
st.title("📊 Dashboard Contele (Formulários)")

# Criar abas principais
tab_dashboard, tab_ia = st.tabs(["📊 Dashboard", "🤖 Conversa com IA"])

# ========== TAB 1: DASHBOARD ==========
with tab_dashboard:
    st.header("📊 Dashboard Analítico")

    # métricas principais em 4 colunas
    st.subheader("Visão Geral dos Dados")
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        try:
            total_formularios = fetch(f"""
                select count(distinct o.task_id) as n 
                from contele.contele_os o
                join contele.contele_answers a using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause}
            """)["n"].iloc[0]
            st.metric("📋 Total de Formulários", int(total_formularios))
        except Exception:
            st.metric("📋 Total de Formulários", "N/A")

    with m2:
        try:
            total_empresas = fetch(f"""
                select count(distinct o.poi) as n 
                from contele.contele_os o
                join contele.contele_answers a using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause} and o.poi is not null
            """)["n"].iloc[0]
            st.metric("🏢 Empresas Visitadas", int(total_empresas))
        except Exception:
            st.metric("🏢 Empresas Visitadas", "N/A")

    with m3:
        try:
            total_vendedores = fetch(f"""
                select count(distinct o.assignee_name) as n 
                from contele.contele_os o
                join contele.contele_answers a using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause} and o.assignee_name is not null
            """)["n"].iloc[0]
            st.metric("👥 Vendedores Ativos", int(total_vendedores))
        except Exception:
            st.metric("👥 Vendedores Ativos", "N/A")

    with m4:
        try:
            media_formularios = fetch(f"""
                select round(
                  (select count(distinct o.task_id)::float
                   from contele.contele_os o
                   join contele.contele_answers a using (task_id)
                   where o.task_id in {TASKS_VIEW}
                     and {where_clause}
                  ) /
                  nullif(
                    (select count(distinct o2.assignee_name)
                     from contele.contele_os o2
                     join contele.contele_answers a2 using (task_id)
                     where o2.task_id in {TASKS_VIEW}
                       and {where_clause}
                       and o2.assignee_name is not null
                    ), 0
                  ), 1) as media
            """)["media"].iloc[0]
            st.metric("📈 Média Formulários/Vendedor", float(media_formularios))
        except Exception:
            st.metric("📈 Média Formulários/Vendedor", "N/A")

    st.divider()

    # gráficos principais em duas colunas
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Distribuição por Tipo de Visita")
        try:
            tipo_visitas = fetch(f"""
                select 
                  case 
                    when a.form_title ilike '%prospec%' then 'Prospecção'
                    when a.form_title ilike '%relat%' then 'Relacionamento'
                    when a.form_title ilike '%levant%' then 'Levantamento'
                    when a.form_title ilike '%visita%' then 'Visita Técnica'
                    else 'Outro'
                  end as tipo,
                  count(distinct o.task_id) as total
                from contele.contele_answers a
                join contele.contele_os o using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by tipo
                order by total desc
            """)
            if not tipo_visitas.empty:
                fig = px.pie(tipo_visitas, values='total', names='tipo', title='Distribuição por Tipo')
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.warning(f"Erro tipo_visitas: {e}")

        st.markdown("### Distribuição por Motivo da Visita")
        try:
            motivo_sql = f"""
                select coalesce(a.answer_human, 'Não informado') as motivo,
                       count(distinct o.task_id) as n
                from contele.contele_answers a
                join contele.contele_os o using(task_id)
                where (lower(a.question_title) like '%motivo%' or lower(a.question_title) like '%objetivo%')
                  and o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by motivo
                order by n desc
                limit 20
            """
            motivo_df = fetch(motivo_sql)
            if not motivo_df.empty:
                st.plotly_chart(px.bar(motivo_df, x='motivo', y='n', title='Distribuição por Motivo', text='n'), use_container_width=True)
                st.dataframe(motivo_df, use_container_width=True)
            else:
                st.info("Nenhum motivo/objetivo identificado nas respostas com os filtros atuais.")
        except Exception as e:
            st.error(f"Erro motivo: {e}")

    with col2:
        st.subheader("Top POIs e Vendedores")
        try:
            pois_df = fetch(f"""
                select o.poi,
                       count(distinct o.task_id) as visitas,
                       max(a.created_at) as ultima_visita
                from contele.contele_os o
                join contele.contele_answers a using(task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by o.poi
                order by visitas desc
                limit 50
            """)
            if not pois_df.empty:
                st.plotly_chart(px.bar(pois_df.head(15), x='poi', y='visitas', title='Top POIs por visitas', text='visitas'), use_container_width=True)
                st.dataframe(pois_df.head(20), use_container_width=True)
            else:
                st.info("Nenhum POI encontrado com os filtros atuais.")
        except Exception as e:
            st.error(f"Erro POIs: {e}")

        try:
            vendedor_visitas = fetch(f"""
                select o.assignee_name, count(distinct o.task_id) as total
                from contele.contele_answers a
                join contele.contele_os o using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause} and o.assignee_name is not null
                group by o.assignee_name
                order by total desc
                limit 15
            """)
            if not vendedor_visitas.empty:
                st.plotly_chart(px.bar(vendedor_visitas, x='assignee_name', y='total', title='Top Vendedores', text='total'), use_container_width=True)
                st.dataframe(vendedor_visitas.head(20), use_container_width=True)
        except Exception as e:
            st.error(f"Erro vendedores: {e}")

    st.divider()

    # ========== TECNOLOGIAS / SOLUÇÕES ==========
    st.subheader("🔬 Tecnologias e Soluções Mencionadas")
    try:
        tech_sql = f"""
            select regexp_split_to_table(lower(coalesce(a.answer_human, '')), '[,;\\s]+') as palavra
            from contele.contele_answers a
            join contele.contele_os o using(task_id)
            where (lower(a.question_title) like '%tecnolog%' or lower(a.question_title) like '%solu%')
              and o.task_id in {TASKS_VIEW}
              and {where_clause}
              and length(coalesce(a.answer_human, '')) > 0
        """
        tech_df = fetch(tech_sql)
        if not tech_df.empty:
            # Filtrar palavras vazias, muito curtas e comuns
            tech_df = tech_df[tech_df['palavra'].str.strip() != '']
            tech_df = tech_df[tech_df['palavra'].str.len() > 3]
            tech_df = tech_df[~tech_df['palavra'].isin(['e', 'ou', 'de', 'para', 'com', 'que', 'não', 'sim', 'na', 'no', 'em', 'um', 'uma'])]
            
            # Contar frequências
            tech_count = tech_df['palavra'].value_counts().head(20).reset_index()
            tech_count.columns = ['palavra', 'frequencia']
            
            if not tech_count.empty:
                fig = px.bar(tech_count, x='frequencia', y='palavra', orientation='h', 
                            title='Top Tecnologias/Soluções', text='frequencia')
                fig.update_layout(xaxis_title='Frequência', yaxis_title='')
                st.plotly_chart(fig, use_container_width=True)
                
                st.markdown("**Top 20 Palavras-chave:**")
                col_a, col_b = st.columns(2)
                for idx, row in tech_count.iterrows():
                    if idx % 2 == 0:
                        col_a.metric(f"#{idx+1}", row['palavra'], f"{int(row['frequencia'])} menções")
                    else:
                        col_b.metric(f"#{idx+1}", row['palavra'], f"{int(row['frequencia'])} menções")
            else:
                st.info("Nenhuma tecnologia/solução identificada com os filtros atuais.")
        else:
            st.info("Nenhuma pergunta sobre tecnologias/soluções encontrada.")
    except Exception as e:
        st.error(f"Erro tecnologias: {e}")

    st.divider()

    # ========== ANÁLISES AVANÇADAS ==========
    with st.expander("📊 Análises Avançadas"):
        st.header("🔎 Análises Avançadas")

        st.subheader("1) Pendências / Oportunidades abertas (por vendedor)")
        try:
            pend_sql = f"""
                select o.assignee_name,
                       count(distinct o.task_id) as pendencias
                from contele.contele_answers a
                join contele.contele_os o using (task_id)
                where (a.question_title ilike '%gerou%' and a.answer_human ilike '%sim%')
                  and o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by o.assignee_name
                order by pendencias desc
                limit 50
            """
            pend_df = fetch(pend_sql)
            if not pend_df.empty:
                st.plotly_chart(px.bar(pend_df, x='assignee_name', y='pendencias', title='Pendências por Vendedor', text='pendencias'), use_container_width=True)
                st.dataframe(pend_df, use_container_width=True)
            else:
                st.info("Nenhuma pendência encontrada.")
        except Exception as e:
            st.error(f"Erro pendências: {e}")

        st.divider()
        st.subheader("2) Tendência temporal (semanal) por tipo")
        try:
            ts_sql = f"""
                select date_trunc('week', a.created_at) as semana,
                       case 
                         when a.form_title ilike '%prospec%' then 'Prospecção'
                         when a.form_title ilike '%relat%' then 'Relacionamento'
                         when a.form_title ilike '%levant%' then 'Levantamento'
                         when a.form_title ilike '%visita%' then 'Visita Técnica'
                         else 'Outro'
                       end as tipo,
                       count(distinct o.task_id) as total
                from contele.contele_answers a
                join contele.contele_os o using (task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by 1,2
                order by 1
            """
            ts_df = fetch(ts_sql)
            if not ts_df.empty:
                ts_df['semana'] = pd.to_datetime(ts_df['semana'])
                fig = px.line(ts_df, x='semana', y='total', color='tipo', markers=True, title='Visitas por semana e tipo')
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(ts_df, use_container_width=True)
            else:
                st.info("Sem dados para série temporal.")
        except Exception as e:
            st.error(f"Erro série temporal: {e}")

        st.divider()
        st.subheader("6) Heatmap: Vendedor x Tipo de Visita")
        try:
            heat_sql = f"""
                select o.assignee_name, 
                       case 
                         when a.form_title ilike '%prospec%' then 'Prospecção'
                         when a.form_title ilike '%relat%' then 'Relacionamento'
                         when a.form_title ilike '%levant%' then 'Levantamento'
                         when a.form_title ilike '%visita%' then 'Visita Técnica'
                         else 'Outro'
                       end as tipo,
                       count(distinct o.task_id) as n
                from contele.contele_answers a
                join contele.contele_os o using(task_id)
                where o.task_id in {TASKS_VIEW}
                  and {where_clause}
                group by 1,2
            """
            heat_df = fetch(heat_sql)
            if not heat_df.empty:
                pivot = heat_df.pivot_table(index='assignee_name', columns='tipo', values='n', fill_value=0)
                fig = px.imshow(pivot.values, x=pivot.columns, y=pivot.index, color_continuous_scale='Blues', labels=dict(x="Tipo", y="Vendedor", color="Visitas"))
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(heat_df, use_container_width=True)
            else:
                st.info("Sem dados para heatmap.")
        except Exception as e:
            st.error(f"Erro heatmap: {e}")

    st.divider()

    # ========== TABELAS BRUTAS ==========
    with st.expander("🔬 Ver Tabelas Brutas"):
        tab1, tab2 = st.tabs(["OS Master", "Respostas"])
        with tab1:
            try:
                os_df = fetch(f"""
                  select task_id, os, poi, title, status, assignee_name, created_at
                  from contele.contele_os
                  order by created_at desc
                  limit 200
                """)
                st.dataframe(os_df, use_container_width=True)
            except Exception as e:
                st.error(f"Erro: {e}")
        with tab2:
            try:
                ans_df = fetch(f"""
                  select task_id, os, poi, form_title, question_title, answer_human, created_at
                  from contele.contele_answers
                  where created_at >= '{data_inicio.isoformat()}'
                    and created_at <= '{data_fim.isoformat()} 23:59:59'
                  order by created_at desc
                  limit 200
                """)
                st.dataframe(ans_df, use_container_width=True)
            except Exception as e:
                st.error(f"Erro: {e}")

# ========== TAB 2: IA CHAT ==========
with tab_ia:
    st.header("🤖 Assistente de Análise IA")
    st.markdown("Seu assistente inteligente para explorar dados e gerar insights em tempo real.")

    if not OPENAI_KEY:
        st.error("❌ Chave OpenAI não configurada! Defina `OPENAI_API_KEY` no arquivo `.env` para usar a IA.")
    elif not client:
        st.error("❌ Erro ao conectar com OpenAI. Verifique sua chave de API.")
    else:
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        
        # Input de usuário - destaque principal
        st.markdown("### 💬 Faça sua pergunta")
        st.info("💡 Digite suas perguntas sobre os dados do dashboard. Pressione **Ctrl+Enter** ou clique em Enviar. A IA usa GPT para fornecer insights contextualizados.")
        
        # Usar form para permitir Enter
        with st.form(key="chat_form", clear_on_submit=True):
            user_msg = st.text_area(
                "Digite sua pergunta sobre os dados:",
                key="ia_input",
                height=150,
                placeholder="Ex.: Qual vendedor tem mais pendências? / Qual é a tendência de visitas? / Quais tecnologias são mais mencionadas? / Qual é o desempenho geral?"
            )
            
            col_send, col_clear = st.columns([4, 1])
            with col_send:
                send = st.form_submit_button("📤 Enviar", use_container_width=True, type="primary")
            with col_clear:
                clear = st.form_submit_button("🔄 Limpar Chat", use_container_width=True)
        
        if clear:
            st.session_state.chat_history = []
            st.rerun()
        
        if send and user_msg.strip():
            # Adicionar pergunta do usuário
            st.session_state.chat_history.append({
                "role": "user",
                "text": user_msg,
                "timestamp": datetime.now()
            })
            
            # Obter resposta da IA
            with st.spinner("🤔 Analisando dados e gerando insights..."):
                ia_response = get_ia_response(user_msg, context)
            
            st.session_state.chat_history.append({
                "role": "assistant",
                "text": ia_response,
                "timestamp": datetime.now()
            })
            st.rerun()
        
        # Exibir histórico do chat (mais recente no topo)
        if st.session_state.chat_history:
            st.divider()
            st.subheader("📋 Conversas Anteriores")
            for msg in reversed(st.session_state.chat_history):
                if msg["role"] == "user":
                    with st.chat_message("user"):
                        st.markdown(msg['text'])
                        st.caption(f"🕐 {msg['timestamp'].strftime('%H:%M:%S')}")
                else:
                    with st.chat_message("assistant"):
                        st.markdown(msg['text'])
                        st.caption(f"🤖 {msg['timestamp'].strftime('%H:%M:%S')}")
        else:
            st.info("💬 Sua conversa com a IA aparecerá aqui. Comece fazendo uma pergunta!")