import os
from pathlib import Path
import base64
import streamlit as st

AUTH_EMAIL = os.getenv("APP_AUTH_EMAIL")
AUTH_PASSWORD = os.getenv("APP_AUTH_PASSWORD")

BASE_DIR = Path(__file__).parent
DEFAULT_LOGO_PATH = BASE_DIR / "img" / "logo.png"
APP_LOGO_PATH = Path(os.getenv("APP_LOGO_PATH", str(DEFAULT_LOGO_PATH)))


def _get_logo_base64() -> str | None:
    """Lê a logo do disco e devolve em base64 para usar no HTML."""
    try:
        with open(APP_LOGO_PATH, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except FileNotFoundError:
        return None


def check_login() -> bool:
    # Se não tem credencial configurada, libera geral
    if not AUTH_EMAIL or not AUTH_PASSWORD:
        st.warning("⚠ Login não configurado — acesso liberado.")
        st.session_state.auth_ok = True
        return True

    # Se já logado, só segue
    if st.session_state.get("auth_ok"):
        return True

    # ================= CSS GLOBAL =================
    st.markdown(
        """
        <style>
        /* Esconde header e sidebar padrão do Streamlit */
        header[data-testid="stHeader"] { display: none !important; }
        section[data-testid="stSidebar"] { display: none !important; }

        html, body {
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
        }

        /* Fundo principal – bem mais neutro, azul só de leve perto da logo */
        [data-testid="stAppViewContainer"] {
            height: 100vh !important;
            background: radial-gradient(
                circle at 14% 0%,
                #111827 0%,   /* azul bem escuro */
                #020617 35%,  /* quase preto */
                #020617 100%
            ) !important;
        }

        /* LOGO NO CANTO SUPERIOR ESQUERDO (SEM CAIXA) */
        .top-logo-wrapper {
            position: fixed;
            top: 24px;
            left: 32px;
            z-index: 9999;
        }

        .top-logo-img {
            width: 210px;
            display: block;
            filter: drop-shadow(0 4px 10px rgba(0,0,0,0.75));
        }

        /* CENTRALIZA O CARD */
        .block-container {
            height: 100vh !important;
            max-width: 460px !important;
            padding: 0 !important;

            display: flex;
            align-items: center;      /* centro vertical */
            justify-content: center;  /* centro horizontal */
        }

        /* Card do formulário */
        div[data-testid="form-container"] {
            background: rgba(11, 16, 30, 0.96);
            border: 1px solid rgba(148,163,184,0.40);
            border-radius: 18px;
            padding: 26px 26px 30px 26px;
            width: 100%;
            max-width: 460px;
            box-shadow: 0 22px 70px rgba(0,0,0,0.70);
            backdrop-filter: blur(14px);
            animation: fadeInScale .28s ease-out forwards;
        }

        @keyframes fadeInScale {
            0% { opacity: 0; transform: scale(0.95); }
            100% { opacity: 1; transform: scale(1); }
        }

        .login-title {
            font-size: 1.6rem;
            font-weight: 700;
            color: #E5E7EB;
            margin-bottom: 2px;
        }

        .login-subtitle {
            font-size: 0.95rem;
            color: #9CA3AF;
            margin-bottom: 14px;
        }

        /* Wrapper dos inputs: alinhar tudo no centro (inclusive o olho) */
        .stTextInput > div {
            display: flex;
            align-items: center;
        }

        .stTextInput > div > div {
            flex: 1;
        }

        .stTextInput > div > div > input {
            background: #111827 !important;
            border-radius: 10px !important;
            height: 40px;
            line-height: 40px;
        }

        .stButton > button {
            width: 100%;
            background: #2563EB !important;
            color: white !important;
            border-radius: 10px !important;
            height: 42px;
            font-size: 16px;
            font-weight: 600;
            margin-top: 8px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ================= LOGO FIXA NO TOPO ESQUERDO =================
    logo_b64 = _get_logo_base64()
    if logo_b64:
        st.markdown(
            f"""
            <div class="top-logo-wrapper">
                <img src="data:image/png;base64,{logo_b64}" class="top-logo-img" />
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ================= FORMULÁRIO (CARD) =================
    with st.form("login_form", clear_on_submit=False):
        st.markdown('<div class="login-title">🔒 Login</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="login-subtitle">Acesse o Dashboard Contele da TecnoTop.</div>',
            unsafe_allow_html=True,
        )

        email = st.text_input("E-mail")
        senha = st.text_input("Senha", type="password")
        submit = st.form_submit_button("Entrar")

    # ================= LÓGICA DE LOGIN =================
    if submit:
        if email == AUTH_EMAIL and senha == AUTH_PASSWORD:
            st.session_state.auth_ok = True
            st.success("✔ Login realizado! Redirecionando…")
            st.rerun()
        else:
            st.error("❌ E-mail ou senha incorretos.")

    st.stop()
