import os
from datetime import datetime, date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from ia_agent import ia_disponivel, responder_pergunta_livre
from login import check_login  # ⬅️ login externo

# ================== ENV / CONFIG ==================
load_dotenv()
if os.path.exists(".env.local"):
    load_dotenv(".env.local", override=True)

DATABASE_URL = os.getenv("DATABASE_URL")

st.set_page_config(page_title="TecnoTop | Contele Forms", layout="wide")

# 🔐 CHAMA O LOGIN ANTES DE QUALQUER OUTRA COISA
check_login()


# ================== HELPERS DB ==================
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
          select 1 
          from information_schema.columns
          where table_schema = '{table_schema}'
            and table_name   = '{table_name}'
            and column_name  = '{column_name}'
          limit 1
        """
        return not fetch(sql).empty
    except Exception:
        return False


def build_context_summary(where_clause_answers: str, TASKS_VIEW: str) -> str:
    """Constrói um resumo completo dos dados para contexto da IA."""
    try:
        summary_data = fetch(
            f"""
            select 
              count(distinct o.task_id) as total_formularios,
              count(distinct o.assignee_name) as total_vendedores,
              count(distinct o.poi) as total_empresas,
              max(a.created_at) as ultima_atualizacao,
              min(a.created_at) as primeira_atualizacao
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause_answers}
        """
        )

        top_vendedores = fetch(
            f"""
            select o.assignee_name, count(distinct o.task_id) as total
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause_answers}
            group by o.assignee_name
            order by total desc
            limit 5
        """
        )

        top_pois = fetch(
            f"""
            select o.poi, count(distinct o.task_id) as total
            from contele.contele_os o
            join contele.contele_answers a using (task_id)
            where o.task_id in {TASKS_VIEW}
              and {where_clause_answers}
            group by o.poi
            order by total desc
            limit 5
        """
        )

        tipos = fetch(
            f"""
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
              and {where_clause_answers}
            group by tipo
        """
        )

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
                context += (
                    f"  {idx+1}. {row['assignee_name']}: {int(row['total'])} formulários\n"
                )
            context += "\n"

        if not top_pois.empty:
            context += "TOP 5 EMPRESAS (POIs):\n"
            for idx, row in top_pois.iterrows():
                context += (
                    f"  {idx+1}. {row['poi']}: {int(row['total'])} visitas\n"
                )
            context += "\n"

        if not tipos.empty:
            context += "DISTRIBUIÇÃO POR TIPO:\n"
            for idx, row in tipos.iterrows():
                context += f"  {row['tipo']}: {int(row['total'])} visitas\n"

        return context
    except Exception as e:
        return f"Erro ao buscar contexto: {str(e)}"


# ================== FILTROS (SIDEBAR) ==================
with st.sidebar:
    st.header("🔍 Filtros")

    st.subheader("📅 Período")

    hoje = datetime.now().date()
    primeiro_dia_ano = date(hoje.year, 1, 1)

    data_inicio = st.date_input(
        "Data Início",
        value=primeiro_dia_ano,
        format="DD/MM/YYYY",
        key="f_data_inicio",
    )
    data_fim = st.date_input(
        "Data Fim",
        value=hoje,
        format="DD/MM/YYYY",
        key="f_data_fim",
    )

    st.divider()
    st.subheader("👥 Equipe")

    try:
        vendedores_list = fetch(
            "select distinct assignee_name from contele.contele_os "
            "where assignee_name is not null order by assignee_name"
        )
        vendedores_options = ["Todos"] + vendedores_list["assignee_name"].tolist()
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
        empresas_list = fetch(
            "select distinct poi from contele.contele_os "
            "where poi is not null order by poi"
        )
        empresas_options = ["Todas"] + empresas_list["poi"].tolist()
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
        [
            "Visão Geral",
            "Prospecção",
            "Relacionamento",
            "Levantamento de Necessidade",
            "Visita Técnica",
        ],
        index=0,
        key="f_objetivo",
    )

    st.divider()
    st.caption("✨ Filtros aplicam-se a todas as análises desta página.")


# ================== WHERE CLAUSES ==================
def build_where_clause_answers() -> str:
    conditions = [
        f"a.created_at >= '{data_inicio.isoformat()}'",
        f"a.created_at <= '{data_fim.isoformat()} 23:59:59'",
    ]

    if "Todos" not in vendedor_selecionado:
        vendedores_str = "', '".join(
            [v.replace("'", "''") for v in vendedor_selecionado]
        )
        conditions.append(f"o.assignee_name IN ('{vendedores_str}')")

    if "Todas" not in empresa_selecionada:
        empresas_str = "', '".join(
            [e.replace("'", "''") for e in empresa_selecionada]
        )
        conditions.append(f"o.poi IN ('{empresas_str}')")

    return " AND ".join(conditions) if conditions else "1=1"


def build_where_clause_visitas() -> str:
    conditions = [
        f"created_at >= '{data_inicio.isoformat()}'",
        f"created_at <= '{data_fim.isoformat()} 23:59:59'",
    ]

    if "Todos" not in vendedor_selecionado:
        vendedores_str = "', '".join(
            [v.replace("'", "''") for v in vendedor_selecionado]
        )
        conditions.append(f"assignee_name IN ('{vendedores_str}')")

    if "Todas" not in empresa_selecionada:
        empresas_str = "', '".join(
            [e.replace("'", "''") for e in empresa_selecionada]
        )
        conditions.append(f"poi IN ('{empresas_str}')")

    return " AND ".join(conditions) if conditions else "1=1"


def build_where_clause_respostas() -> str:
    conditions = [
        f"os_created_at >= '{data_inicio.isoformat()}'",
        f"os_created_at <= '{data_fim.isoformat()} 23:59:59'",
    ]

    if "Todos" not in vendedor_selecionado:
        vendedores_str = "', '".join(
            [v.replace("'", "''") for v in vendedor_selecionado]
        )
        conditions.append(f"assignee_name IN ('{vendedores_str}')")

    if "Todas" not in empresa_selecionada:
        empresas_str = "', '".join(
            [e.replace("'", "''") for e in empresa_selecionada]
        )
        conditions.append(f"poi IN ('{empresas_str}')")

    return " AND ".join(conditions) if conditions else "1=1"


where_clause_answers = build_where_clause_answers()
where_clause_visitas = build_where_clause_visitas()
where_clause_respostas = build_where_clause_respostas()

# where_port para portfólio
where_port = "1=1"
if "Todas" not in empresa_selecionada:
    empresas_str = "', '".join(
        [e.replace("'", "''") for e in empresa_selecionada]
    )
    where_port = f"poi IN ('{empresas_str}')"

# ================== TASKS VIEW (POR OBJETIVO) ==================
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

# Contexto para IA
context = build_context_summary(where_clause_answers, TASKS_VIEW)


# ================== CARREGAMENTO ÚNICO DE BASES ==================
@st.cache_data(ttl=120, show_spinner=False)
def load_bases(where_visitas: str, where_respostas: str, where_port: str):
    """
    Carrega de uma vez:
    - vw_visitas_status (visitas)
    - vw_todas_os_respostas (respostas de formulário)
    - vw_portfolio_clientes (portfolio Festo/Bosch/Hengst/Wago)
    """
    engine = get_engine()
    with engine.connect() as c:
        df_visitas = pd.read_sql(
            text(f"""
                SELECT *
                FROM contele.vw_visitas_status
                WHERE {where_visitas}
            """),
            c,
        )

        df_respostas = pd.read_sql(
            text(f"""
                SELECT *
                FROM contele.vw_todas_os_respostas
                WHERE {where_respostas}
            """),
            c,
        )

        df_portfolio = pd.read_sql(
            text(f"""
                SELECT *
                FROM contele.vw_portfolio_clientes
                WHERE {where_port}
                ORDER BY poi
                LIMIT 200
            """),
            c,
        )

    return df_visitas, df_respostas, df_portfolio


# ================== LAYOUT PRINCIPAL ==================
st.title("📊 Dashboard Contele (Formulários)")

tab_dashboard, tab_ia = st.tabs(["📊 Dashboard", "🤖 Conversa com IA"])

# ================== TAB 1: DASHBOARD ==================
with tab_dashboard:
    st.header("📊 Dashboard Analítico")

    # ===== CARREGA TUDO DE UMA VEZ =====
    with st.spinner("Carregando dados para o período e filtros selecionados..."):
        df_visitas, df_respostas, df_portfolio = load_bases(
            where_clause_visitas,
            where_clause_respostas,
            where_port,
        )

    # Garantir tipos de data
    if not df_visitas.empty and "created_at" in df_visitas.columns:
        if not pd.api.types.is_datetime64_any_dtype(df_visitas["created_at"]):
            df_visitas["created_at"] = pd.to_datetime(df_visitas["created_at"])

    if not df_respostas.empty and "os_created_at" in df_respostas.columns:
        if not pd.api.types.is_datetime64_any_dtype(df_respostas["os_created_at"]):
            df_respostas["os_created_at"] = pd.to_datetime(df_respostas["os_created_at"])

    # ---------- MÉTRICAS PRINCIPAIS ----------
    st.subheader("Visão Geral dos Dados")
    m1, m2, m3 = st.columns(3)

    # --- Identifica abordagens sem sucesso via df_respostas ---
    df_abord = pd.DataFrame()
    ids_abord = set()

    if not df_respostas.empty and "form_title" in df_respostas.columns:
        df_abord = df_respostas[
            df_respostas["form_title"].fillna("").str.lower() == "abordagem sem sucesso"
        ].copy()

        if "task_id" in df_abord.columns:
            ids_abord = set(df_abord["task_id"].unique())

    # Garante coluna booleana em df_visitas marcando abordagens sem sucesso
    if "task_id" in df_visitas.columns:
        df_visitas["is_abordagem_sem_sucesso"] = df_visitas["task_id"].isin(ids_abord)
    else:
        df_visitas["is_abordagem_sem_sucesso"] = False

    total_visitas_total = len(df_visitas)
    abordagens_sem_sucesso = int(df_visitas["is_abordagem_sem_sucesso"].sum())
    visitas_efetivas = total_visitas_total - abordagens_sem_sucesso

    # KPI 1: Total de visitas (apenas efetivas)
    with m1:
        st.metric("📋 Total de Visitas", int(visitas_efetivas))

    # KPI 2: Taxa de visitas efetivas (sobre o total geral)
    with m2:
        base_total = visitas_efetivas + abordagens_sem_sucesso
        if base_total > 0:
            taxa = visitas_efetivas * 100.0 / base_total
        else:
            taxa = 0.0
        st.metric("✅ Taxa de Visitas Efetivas", f"{taxa:.1f}%")

    # KPI 3: Abordagens sem sucesso
    with m3:
        st.metric("⚠️ Abordagens sem Sucesso", int(abordagens_sem_sucesso))

    # variável de base total para outros cálculos (ex.: proporções)
    total_visitas = total_visitas_total

    st.divider()

    # ---------- GRÁFICOS PRINCIPAIS ----------
    col1, col2 = st.columns([2, 1])

    # COLUNA ESQUERDA
    with col1:
        st.subheader("Visitas por Objetivo (Qtd)")
        try:
            if not df_visitas.empty and "objetivo" in df_visitas.columns:
                objetivo_df = df_visitas.copy()

                if "is_abordagem_sem_sucesso" not in objetivo_df.columns:
                    objetivo_df["is_abordagem_sem_sucesso"] = False

                objetivo_df["objetivo_legenda"] = (
                    objetivo_df["objetivo"]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                )

                # marca explicitamente as abordagens sem sucesso
                objetivo_df.loc[
                    objetivo_df["is_abordagem_sem_sucesso"], "objetivo_legenda"
                ] = "Abordagens sem Sucesso"

                # objetivos vazios mas efetivos
                objetivo_df.loc[
                    (~objetivo_df["is_abordagem_sem_sucesso"])
                    & (objetivo_df["objetivo_legenda"] == ""),
                    "objetivo_legenda",
                ] = "Sem objetivo informado"

                objetivo_df = (
                    objetivo_df.groupby("objetivo_legenda", as_index=False)
                    .size()
                    .rename(columns={"size": "total"})
                    .sort_values("total", ascending=False)
                )

                if not objetivo_df.empty:
                    fig = px.pie(
                        objetivo_df,
                        values="total",
                        names="objetivo_legenda",
                        title="Visitas por Objetivo (Qtd)",
                    )
                    fig.update_traces(
                        textposition="inside",
                        textinfo="value+percent",
                        insidetextorientation="radial",
                        hovertemplate="%{label}<br>%{value} visitas (%{percent})<extra></extra>",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Sem visitas para o filtro atual.")
            else:
                st.info("Sem visitas para o filtro atual.")
        except Exception as e:
            st.warning(f"Erro: {e}")

        # Motivos de insucesso (Top 10)
        st.markdown("### Motivos de Insucesso (Top 10)")
        try:
            if not df_respostas.empty:
                qt = df_respostas["question_title"].fillna("").str.lower()
                # trata "Situação Encontrada" com e sem acento
                mask_title = (
                    qt.str.startswith("situação encontrada")
                    | qt.str.startswith("situacao encontrada")
                )

                mask_motivos = (
                    df_respostas["form_title"].fillna("").str.lower().eq("abordagem sem sucesso")
                    & mask_title
                    & df_respostas["answer_human"].notna()
                    & (df_respostas["answer_human"].astype(str).str.strip() != "")
                )

                motivos_df = (
                    df_respostas[mask_motivos]
                    .assign(
                        motivo=lambda d: d["answer_human"]
                        .astype(str)
                        .str.lower()
                        .str.strip()
                    )
                )

                if "task_id" in motivos_df.columns:
                    motivos_df = (
                        motivos_df.groupby("motivo", as_index=False)["task_id"]
                        .nunique()
                        .rename(columns={"task_id": "total"})
                        .sort_values("total", ascending=False)
                        .head(10)
                    )
                else:
                    motivos_df = pd.DataFrame(columns=["motivo", "total"])

                if not motivos_df.empty:
                    st.plotly_chart(
                        px.bar(
                            motivos_df,
                            x="motivo",
                            y="total",
                            title="Top Motivos de Insucesso",
                            text="total",
                        ),
                        use_container_width=True,
                    )
                else:
                    st.info("Nenhuma abordagem sem sucesso no período.")
            else:
                st.info("Nenhuma abordagem sem sucesso no período.")
        except Exception as e:
            st.error(f"Erro: {e}")

    # COLUNA DIREITA
    with col2:
        st.subheader("Top POIs e Vendedores")

        # Top POIs
        try:
            if not df_visitas.empty and "poi" in df_visitas.columns:
                pois_df = (
                    df_visitas[df_visitas["poi"].notna()]
                    .groupby("poi", as_index=False)
                    .size()
                    .rename(columns={"size": "visitas"})
                    .sort_values("visitas", ascending=False)
                    .head(15)
                )
                if not pois_df.empty:
                    st.plotly_chart(
                        px.bar(
                            pois_df,
                            x="poi",
                            y="visitas",
                            title="Top POIs",
                            text="visitas",
                        ),
                        use_container_width=True,
                    )
        except Exception as e:
            st.error(f"Erro: {e}")

        # Top vendedores
        try:
            if not df_visitas.empty and "assignee_name" in df_visitas.columns:
                vendedor_visitas = (
                    df_visitas[df_visitas["assignee_name"].notna()]
                    .groupby("assignee_name", as_index=False)
                    .size()
                    .rename(columns={"size": "total"})
                    .sort_values("total", ascending=False)
                    .head(15)
                )
                if not vendedor_visitas.empty:
                    st.plotly_chart(
                        px.bar(
                            vendedor_visitas,
                            x="assignee_name",
                            y="total",
                            title="Top Vendedores",
                            text="total",
                        ),
                        use_container_width=True,
                    )
        except Exception as e:
            st.error(f"Erro: {e}")

    st.divider()

    # ---------- VISITAS MENSAIS ----------
    st.subheader("Visitas Mensais Realizadas")

    try:
        if not df_visitas.empty and "created_at" in df_visitas.columns:
            df_visitas_m = df_visitas.copy()
            df_visitas_m["mes"] = df_visitas_m["created_at"].dt.to_period("M").dt.to_timestamp()
            visitas_mensais_df = (
                df_visitas_m.groupby("mes", as_index=False)
                .size()
                .rename(columns={"size": "total_visitas"})
            )
        else:
            visitas_mensais_df = pd.DataFrame(columns=["mes", "total_visitas"])

        if not df_respostas.empty and "os_created_at" in df_respostas.columns:
            df_abord_m = df_respostas[
                df_respostas["form_title"].fillna("").str.lower().eq("abordagem sem sucesso")
            ].copy()
            df_abord_m["mes"] = df_abord_m["os_created_at"].dt.to_period("M").dt.to_timestamp()

            if "task_id" in df_abord_m.columns:
                abord_df = (
                    df_abord_m.groupby("mes", as_index=False)["task_id"]
                    .nunique()
                    .rename(columns={"task_id": "abordagens_sem_sucesso"})
                )
            else:
                abord_df = pd.DataFrame(columns=["mes", "abordagens_sem_sucesso"])
        else:
            abord_df = pd.DataFrame(columns=["mes", "abordagens_sem_sucesso"])

        visitas_mensais_df = visitas_mensais_df.merge(
            abord_df, on="mes", how="left"
        ).fillna({"abordagens_sem_sucesso": 0})

        visitas_mensais_df["visitas_efetivas"] = (
            visitas_mensais_df["total_visitas"] - visitas_mensais_df["abordagens_sem_sucesso"]
        )

        if not visitas_mensais_df.empty:
            tl = visitas_mensais_df.rename(
                columns={
                    "mes": "mes",
                    "total_visitas": "Total de visitas",
                    "abordagens_sem_sucesso": "Abordagens sem sucesso",
                    "visitas_efetivas": "Visitas efetivas",
                }
            )
            fig_tl = px.line(
                tl,
                x="mes",
                y=[
                    "Total de visitas",
                    "Abordagens sem sucesso",
                    "Visitas efetivas",
                ],
                markers=True,
                labels={"value": "Visitas", "mes": "Mês", "variable": "Tipo"},
                title="Visitas Mensais (Total x Abordagens sem sucesso x Efetivas)",
            )
            st.plotly_chart(fig_tl, use_container_width=True)
        else:
            st.info("Sem dados de visitas mensais para o período/filtros selecionados.")
    except Exception as e:
        st.error(f"Erro ao carregar visitas mensais: {e}")

    st.divider()

    # ---------- INSIGHTS COMERCIAIS E RELACIONAMENTO ----------
    st.subheader("🔍 Insights Comerciais e de Relacionamento")

    # Visitas em conjunto com braço interno
    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("#### Visitas em conjunto com braço interno")

        try:
            visitas_conjuntas = 0
            dist_df = pd.DataFrame()

            if not df_respostas.empty:
                base = df_respostas[
                    df_respostas["question_title"]
                    .fillna("")
                    .str.contains("visita foi feita em conjunto", case=False)
                ].copy()

                if not base.empty:
                    def classifica_braco(x: str) -> str:
                        t = str(x).lower()
                        if "engenharia" in t:
                            return "Engenharia"
                        if "service" in t:
                            return "Service"
                        if "não se aplica" in t or "nao se aplica" in t:
                            return "Não se aplica"
                        return "Outros"

                    base["braco_interno"] = base["answer_human"].apply(classifica_braco)
                    base = base.drop_duplicates(subset=["task_id", "braco_interno"])

                    visitas_conjuntas = int(
                        base[base["braco_interno"] != "Não se aplica"]["task_id"]
                        .nunique()
                    )

                    dist_df = (
                        base[base["braco_interno"] != "Não se aplica"]
                        .groupby("braco_interno", as_index=False)["task_id"]
                        .nunique()
                        .rename(columns={"task_id": "qtd_visitas"})
                        .sort_values("qtd_visitas", ascending=False)
                    )

            proporcao = (
                (visitas_conjuntas / total_visitas * 100.0)
                if total_visitas > 0
                else 0.0
            )

            m_conj1, m_conj2 = st.columns(2)
            m_conj1.metric("🤝 Visitas em conjunto", visitas_conjuntas)
            m_conj2.metric("📌 Proporção sobre o total", f"{proporcao:.1f}%")
            st.caption(f"Base de {total_visitas} visitas no período.")
        except Exception as e:
            st.error(f"Erro ao carregar visitas em conjunto: {e}")

    with c2:
        try:
            if not dist_df.empty:
                fig_braco = px.bar(
                    dist_df,
                    x="braco_interno",
                    y="qtd_visitas",
                    title="Distribuição das visitas por área interna",
                    text="qtd_visitas",
                )
                fig_braco.update_layout(
                    xaxis_title="Braço interno",
                    yaxis_title="Qtd de visitas",
                )
                st.plotly_chart(fig_braco, use_container_width=True)
            else:
                st.info("Sem visitas em conjunto registradas para o filtro atual.")
        except Exception as e:
            st.error(f"Erro ao carregar distribuição por área interna: {e}")

    st.divider()

    # ---------- PERFIL COMERCIAL DOS CLIENTES ----------
    st.subheader("📌 Perfil comercial dos clientes")

    # Segmento do cliente (texto reduzido)
    try:
        if not df_respostas.empty:
            seg_mask = (
                df_respostas["question_title"]
                .fillna("")
                .str.contains("segmento do cliente", case=False)
                & df_respostas["answer_human"].notna()
                & (df_respostas["answer_human"].str.strip() != "")
            )
            seg_df = df_respostas[seg_mask].copy()

            if not seg_df.empty:
                seg_df["segmento"] = (
                    seg_df["answer_human"]
                    .astype(str)
                    .str.split(" - ")
                    .str[0]
                    .str.strip()
                    .str.lower()
                )
                if "poi" in seg_df.columns:
                    seg_df = (
                        seg_df.groupby("segmento", as_index=False)["poi"]
                        .nunique()
                        .rename(columns={"poi": "qtd_clientes"})
                        .sort_values("qtd_clientes", ascending=False)
                        .head(10)
                    )
                else:
                    seg_df = pd.DataFrame(columns=["segmento", "qtd_clientes"])
            else:
                seg_df = pd.DataFrame(columns=["segmento", "qtd_clientes"])
        else:
            seg_df = pd.DataFrame(columns=["segmento", "qtd_clientes"])
    except Exception:
        seg_df = pd.DataFrame(columns=["segmento", "qtd_clientes"])

    # Função no processo de compras
    try:
        if not df_respostas.empty:
            func_mask = (
                df_respostas["question_title"].fillna("").str.contains("funç", case=False)
                & df_respostas["question_title"].fillna("").str.contains("processo de compra", case=False)
                & df_respostas["answer_human"].notna()
                & (df_respostas["answer_human"].str.strip() != "")
            )
            func_df = df_respostas[func_mask].copy()

            if not func_df.empty:
                func_df["funcao"] = (
                    func_df["answer_human"]
                    .astype(str)
                    .str.lower()
                    .str.strip()
                )
                if "poi" in func_df.columns:
                    func_df = (
                        func_df.groupby("funcao", as_index=False)["poi"]
                        .nunique()
                        .rename(columns={"poi": "qtd_clientes"})
                        .sort_values("qtd_clientes", ascending=False)
                    )
                else:
                    func_df = pd.DataFrame(columns=["funcao", "qtd_clientes"])
            else:
                func_df = pd.DataFrame(columns=["funcao", "qtd_clientes"])
        else:
            func_df = pd.DataFrame(columns=["funcao", "qtd_clientes"])
    except Exception:
        func_df = pd.DataFrame(columns=["funcao", "qtd_clientes"])

    # Tecnologias / soluções que podem agregar
    try:
        if not df_respostas.empty:
            sol_mask = (
                df_respostas["question_title"].fillna("").str.contains("tecnolog", case=False)
                & df_respostas["question_title"].fillna("").str.contains("agregar", case=False)
                & df_respostas["answer_human"].notna()
                & (df_respostas["answer_human"].str.strip() != "")
            )
            sol_df = df_respostas[sol_mask].copy()

            if not sol_df.empty:
                def class_sol(x: str) -> str:
                    t = str(x).lower()
                    if "festo" in t:
                        return "festo"
                    if "wago" in t:
                        return "wago"
                    if "bosch" in t or "rexroth" in t:
                        return "bosch rexroth"
                    if "hengst" in t:
                        return "hengst"
                    return "outro"

                sol_df["solucao"] = sol_df["answer_human"].apply(class_sol)
                sol_df = (
                    sol_df.groupby("solucao", as_index=False)
                    .size()
                    .rename(columns={"size": "ocorrencias"})
                    .sort_values("ocorrencias", ascending=False)
                )
            else:
                sol_df = pd.DataFrame(columns=["solucao", "ocorrencias"])
        else:
            sol_df = pd.DataFrame(columns=["solucao", "ocorrencias"])
    except Exception:
        sol_df = pd.DataFrame(columns=["solucao", "ocorrencias"])

    c_seg, c_func, c_sol = st.columns(3)

    with c_seg:
        st.markdown("#### Segmento do cliente")
        if not seg_df.empty:
            fig_seg = px.bar(
                seg_df,
                x="segmento",
                y="qtd_clientes",
                text="qtd_clientes",
            )
            fig_seg.update_layout(xaxis_title="segmento", yaxis_title="qtd. clientes")
            st.plotly_chart(fig_seg, use_container_width=True)
        else:
            st.info("Sem dados de segmento para o filtro atual.")

    with c_func:
        st.markdown("#### Função do contato no processo de compras")
        if not func_df.empty:
            fig_func = px.bar(
                func_df,
                x="funcao",
                y="qtd_clientes",
                text="qtd_clientes",
            )
            fig_func.update_layout(xaxis_title="função", yaxis_title="qtd. clientes")
            st.plotly_chart(fig_func, use_container_width=True)
        else:
            st.info("Sem dados de função do contato para o filtro atual.")

    with c_sol:
        st.markdown("#### Tecnologias / soluções que podem agregar")
        if not sol_df.empty:
            counts = {row["solucao"]: int(row["ocorrencias"]) for _, row in sol_df.iterrows()}
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Festo (citações)", counts.get("festo", 0))
            k2.metric("Bosch Rexroth", counts.get("bosch rexroth", 0))
            k3.metric("Wago", counts.get("wago", 0))
            k4.metric("Hengst", counts.get("hengst", 0))

            fig_sol = px.bar(
                sol_df,
                x="solucao",
                y="ocorrencias",
                text="ocorrencias",
            )
            fig_sol.update_layout(xaxis_title="solução", yaxis_title="ocorrências")
            st.plotly_chart(fig_sol, use_container_width=True)
        else:
            st.info("Sem dados de soluções para o filtro atual.")

    st.divider()

    # ---------- CONCORRENTES ----------
    st.subheader("Concorrentes que atuam nos clientes")

    try:
        if not df_respostas.empty:
            conc_mask = (
                df_respostas["question_title"]
                .fillna("")
                .str.contains("concorrente", case=False)
                & df_respostas["answer_human"].notna()
                & (df_respostas["answer_human"].str.strip() != "")
            )
            conc_df = df_respostas[conc_mask].copy()

            if not conc_df.empty:
                conc_df["concorrente"] = (
                    conc_df["answer_human"]
                    .astype(str)
                    .str.lower()
                    .str.strip()
                )

                def limpa_concorrente(x: str) -> str:
                    t = x.strip().lower()
                    sem_info_terms = [
                        "não sei", "nao sei",
                        "não tem", "nao tem",
                        "não informado", "nao informado",
                        "não informaram", "nao informaram",
                        "não mencionado", "nao mencionado",
                        "não mencionou", "nao mencionou",
                        "não conversado", "nao conversado",
                        "nenhum", "nenhuma", "nada",
                    ]
                    if any(t.startswith(s) for s in sem_info_terms):
                        return "sem informação"
                    if len(t) > 22:
                        return "outros"
                    return t or "sem informação"

                conc_df["concorrente_clean"] = conc_df["concorrente"].apply(limpa_concorrente)

                if "poi" in conc_df.columns:
                    conc_group = (
                        conc_df.groupby("concorrente_clean", as_index=False)["poi"]
                        .nunique()
                        .rename(columns={"poi": "qtd_clientes"})
                        .sort_values("qtd_clientes", ascending=False)
                    )
                else:
                    conc_group = pd.DataFrame(columns=["concorrente_clean", "qtd_clientes"])

                if not conc_group.empty:
                    fig_conc = px.bar(
                        conc_group,
                        x="qtd_clientes",
                        y="concorrente_clean",
                        orientation="h",
                        text="qtd_clientes",
                        labels={
                            "qtd_clientes": "Qtd de clientes em que aparece",
                            "concorrente_clean": "concorrente",
                        },
                        title="Concorrentes mais presentes na base de clientes",
                    )
                    fig_conc.update_layout(
                        yaxis={"categoryorder": "total ascending"},
                    )
                    st.plotly_chart(fig_conc, use_container_width=True)
                else:
                    st.info("Sem dados de concorrentes para o filtro atual.")
            else:
                st.info("Sem dados de concorrentes para o filtro atual.")
        else:
            st.info("Sem dados de concorrentes para o filtro atual.")
    except Exception as e:
        st.error(f"Erro ao carregar concorrentes: {e}")

    st.divider()

    # ---------- MAPA DE NECESSIDADES DOS CLIENTES ----------
    st.subheader("Mapa de Necessidades dos Clientes")

    try:
        if not df_respostas.empty:
            mask_nec = (
                df_respostas["answer_human"].notna()
                & (df_respostas["answer_human"].str.strip() != "")
                & (
                    df_respostas["question_title"].fillna("").str.contains("problema", case=False)
                    | df_respostas["question_title"].fillna("").str.contains("desafio", case=False)
                    | df_respostas["question_title"].fillna("").str.contains("manuten", case=False)
                    | df_respostas["question_title"].fillna("").str.contains("oportun", case=False)
                )
            )
            necessidades_df = df_respostas[mask_nec].copy()

            if not necessidades_df.empty:
                def cat_nec(q: str) -> str:
                    qt = str(q).lower()
                    if "problema" in qt:
                        return "Problemas relatados"
                    if "desafio" in qt:
                        return "Desafios apresentados"
                    if "manuten" in qt:
                        return "Demandas de manutenção"
                    if "oportun" in qt:
                        return "Oportunidades de melhoria"
                    return "Outros"

                necessidades_df["categoria"] = necessidades_df["question_title"].apply(cat_nec)
                necessidades_df["tema"] = (
                    necessidades_df["answer_human"]
                    .astype(str)
                    .str.lower()
                    .str.strip()
                )

                lixo = [
                    "sim", "não", "nao",
                    "nenhum", "nenhuma",
                    "nada",
                    "não mencionou", "nao mencionou",
                    "não informou", "nao informou",
                    "sem problemas", "sem problema",
                ]
                necessidades_df = necessidades_df[
                    ~necessidades_df["tema"].isin(lixo)
                ].copy()

                if not necessidades_df.empty:
                    nec_group = (
                        necessidades_df.groupby(["categoria", "tema"], as_index=False)
                        .size()
                        .rename(columns={"size": "total"})
                        .sort_values("total", ascending=False)
                        .head(50)
                    )

                    def corta(t: str) -> str:
                        return t if len(t) <= 80 else t[:80] + "..."

                    nec_group["tema_curto"] = nec_group["tema"].apply(corta)

                    c_left, c_right = st.columns([1.5, 1])

                    with c_left:
                        top_plot = nec_group.head(20)
                        fig_nec = px.bar(
                            top_plot,
                            x="total",
                            y="tema_curto",
                            color="categoria",
                            orientation="h",
                            title="Ranking de Temas Mais Citados",
                            hover_data={"tema": True, "tema_curto": False},
                        )
                        fig_nec.update_layout(yaxis={"categoryorder": "total ascending"})
                        st.plotly_chart(fig_nec, use_container_width=True)

                    with c_right:
                        st.dataframe(nec_group, use_container_width=True)
                else:
                    st.info(
                        "Nenhuma necessidade relevante após limpeza de respostas genéricas."
                    )
            else:
                st.info(
                    "Nenhuma necessidade encontrada nas respostas de problemas/desafios/manutenção/oportunidades para o filtro atual."
                )
        else:
            st.info(
                "Nenhuma necessidade encontrada nas respostas para o filtro atual."
            )
    except Exception as e:
        st.error(f"Erro ao carregar mapa de necessidades: {e}")

    st.divider()

    # ---------- PENETRAÇÃO DE PORTFÓLIO ----------
    st.subheader("Penetração de Portfólio por Cliente (Festo / Bosch / Hengst / Wago)")
    try:
        if not df_portfolio.empty:
            st.dataframe(df_portfolio, use_container_width=True)
        else:
            st.info("Nenhum dado de portfólio encontrado para o filtro atual.")
    except Exception as e:
        st.error(f"Erro: {e}")

# ================== TAB 2: IA CHAT INTEGRADO ==================
with tab_ia:
    st.header("🤖 Assistente de Análise IA")
    st.markdown("Chat integrado com acesso total aos seus dados. Converse naturalmente.")

    disponivel, msg_status = ia_disponivel()

    if not disponivel:
        st.error(msg_status)
        st.info("💡 Configure `OPENAI_API_KEY` no arquivo `.env`")
    else:
        filtros_atuais = {
            "data_inicio": data_inicio.strftime("%d/%m/%Y"),
            "data_fim": data_fim.strftime("%d/%m/%Y"),
            "vendedores": ", ".join(vendedor_selecionado),
            "empresas": ", ".join(empresa_selecionada),
            "tipo_visita": objetivo,
        }

        # memória da sessão do Streamlit
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []

        st.markdown("### 💬 Converse com a IA")

        with st.expander("💡 Exemplos do que perguntar", expanded=False):
            st.markdown(
                """
            - "Faça um resumo executivo dos dados atuais"
            - "Quais vendedores precisam de mais atenção?"
            - "Monte um plano de ação para os próximos 7 dias"
            - "Onde estão as maiores oportunidades?"
            - "Analise as pendências em aberto"
            """
            )

        with st.form(key="chat_form", clear_on_submit=True):
            user_msg = st.text_area(
                "Digite sua mensagem:",
                key="ia_input",
                height=120,
                placeholder="Ex.: Faça um resumo executivo e sugira 3 ações prioritárias...",
            )

            col_send, col_clear = st.columns([4, 1])
            with col_send:
                enviar = st.form_submit_button(
                    "📤 Enviar", use_container_width=True, type="primary"
                )
            with col_clear:
                limpar = st.form_submit_button("🗑️ Limpar", use_container_width=True)

        if limpar:
            st.session_state.chat_history = []
            st.rerun()  # ✅ substitui st.experimental_rerun()

        if enviar and user_msg.strip():
            st.session_state.chat_history.append(
                {
                    "role": "user",
                    "text": user_msg,
                    "timestamp": datetime.now(),
                }
            )

            MAX_HISTORY_MSGS = 20
            history_for_ia = [
                {"role": msg["role"], "text": msg["text"]}
                for msg in st.session_state.chat_history[-MAX_HISTORY_MSGS:]
            ]

            with st.spinner("🤔 IA analisando e respondendo..."):
                resposta = responder_pergunta_livre(
                    user_msg,
                    context=context,
                    filters=filtros_atuais,
                    history=history_for_ia,
                )

            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "text": resposta,
                    "timestamp": datetime.now(),
                }
            )
            st.rerun()  # ✅ substitui st.experimental_rerun()

        st.divider()

        if st.session_state.chat_history:
            st.subheader("💬 Conversa")

            for msg in reversed(st.session_state.chat_history):
                if msg["role"] == "user":
                    with st.chat_message("user", avatar="👤"):
                        st.markdown(msg["text"])
                        st.caption(
                            f"🕐 {msg['timestamp'].strftime('%d/%m/%Y %H:%M:%S')}"
                        )
                else:
                    with st.chat_message("assistant", avatar="🤖"):
                        st.markdown(msg["text"])
                        st.caption(
                            f"🤖 {msg['timestamp'].strftime('%d/%m/%Y %H:%M:%S')}"
                        )
        else:
            st.info("👋 **Bem-vindo!** Comece fazendo uma pergunta sobre seus dados.")
