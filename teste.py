#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, math, logging, datetime as dt
from typing import Dict, Any, Iterable, List, Tuple, Optional

import requests
import psycopg2, psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# --------- ENV ---------
DATABASE_URL   = os.getenv("DATABASE_URL")
V2_BASE        = (os.getenv("CONTELE_V2_BASE") or "").rstrip("/")
V2_AUTH        = (os.getenv("CONTELE_V2_AUTHORIZATION") or "").strip()
V2_KEY         = (os.getenv("CONTELE_V2_X_API_KEY") or "").strip()
FORMS_BASE     = (os.getenv("CONTELE_FORMS_BASE") or "").rstrip("/")
FORMS_BEARER   = (os.getenv("CONTELE_FORMS_BEARER") or "").strip()
SINCE          = os.getenv("SINCE") or f"{dt.date.today().year}-01-01"
TO             = os.getenv("TO")    or f"{dt.date.today().year}-12-31"
TZ             = os.getenv("TZ")    or "America/Sao_Paulo"
PER_PAGE       = int(os.getenv("PER_PAGE") or "100")

SESSION = requests.Session()
DEFAULT_TIMEOUT = 60

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def now_utc_iso() -> str:
    return now_utc().isoformat()

def parse_ts(s: Optional[str]) -> dt.datetime:
    if not s:
        return dt.datetime(1970,1,1, tzinfo=dt.timezone.utc)
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)
    except Exception:
        return dt.datetime(1970,1,1, tzinfo=dt.timezone.utc)

def http_get(url: str, headers: Dict[str,str], params: Dict[str,str],
             ok_codes=(200,), max_retries=4, backoff=0.8):
    for attempt in range(max_retries):
        r = SESSION.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
        if r.status_code in ok_codes:
            return r
        if r.status_code in (401,403):
            return r
        if r.status_code in (429,500,502,503,504):
            wait = backoff * (2 ** attempt)
            logging.warning(f"GET {url} => {r.status_code}. Retry em {wait:.1f}s…")
            time.sleep(wait)
            continue
        return r
    return r

def v2_headers():
    return {
        "Authorization": f"Bearer {V2_AUTH}",
        "x-api-key": V2_KEY,
        "Accept":"application/json",
        "User-Agent":"TecnoTop-ConteleSync/1.0"
    }

def forms_headers():
    return {
        "Authorization": f"Bearer {FORMS_BEARER}",
        "Accept":"application/json",
        "User-Agent":"TecnoTop-ConteleSync/1.0"
    }

def dedup_last(rows: List[Dict[str,Any]], key_fields: Tuple[str, ...],
               ts_field: Optional[str] = None) -> List[Dict[str,Any]]:
    bucket: Dict[Tuple[Any,...], Dict[str,Any]] = {}
    for r in rows:
        key = tuple(r.get(k) for k in key_fields)
        if ts_field:
            best = bucket.get(key)
            if best is None or parse_ts(r.get(ts_field)) >= parse_ts(best.get(ts_field)):
                bucket[key] = r
        else:
            bucket[key] = r
    return list(bucket.values())

# --------- DB bootstrap (schema/tabelas/índices/views) ---------
DDL_BOOTSTRAP = r"""
CREATE SCHEMA IF NOT EXISTS contele;

CREATE TABLE IF NOT EXISTS contele.contele_os (
  task_id          text PRIMARY KEY,
  os               text,
  poi              text,
  title            text,
  status           text,
  assignee_name    text,
  assignee_id      text,
  created_at       timestamptz,
  finished_at      timestamptz,
  updated_at       timestamptz,
  ingested_at      timestamptz,
  updated_at_local timestamptz
);

CREATE TABLE IF NOT EXISTS contele.contele_answers (
  task_id        text NOT NULL,
  os             text,
  poi            text,
  form_title     text,
  question_id    text NOT NULL,
  question_title text,
  answer_human   text,
  answer_raw     text,
  created_at     timestamptz,
  ingested_at    timestamptz,
  PRIMARY KEY(task_id, question_id)
);

-- helper: normaliza question_title pra virar nome de coluna (truncado)
CREATE OR REPLACE FUNCTION contele._slug(t text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT left(
    regexp_replace(
      regexp_replace(lower(coalesce($1,'')), '[^a-z0-9]+', '_', 'g'),
      '^_|_$','', 'g'
    ),
    50
  )
$$;

-- Função que (re)cria view WIDE (todas as respostas em colunas)
-- Inclui OS, POI e responsáveis. SEM filtro de objetivo.
CREATE OR REPLACE FUNCTION contele.rebuild_view_all(view_name text)
RETURNS void
LANGUAGE plpgsql
AS $FN$
DECLARE
  sql_cols text := '';
  rec record;
BEGIN
  FOR rec IN
    WITH distinct_titles AS (
      SELECT DISTINCT a.question_title
      FROM contele.contele_answers a
      WHERE a.question_title IS NOT NULL
    ), base AS (
      SELECT
        question_title,
        contele._slug(question_title) AS slug,
        ROW_NUMBER() OVER (PARTITION BY contele._slug(question_title) ORDER BY question_title) AS rn
      FROM distinct_titles
    )
    SELECT
      question_title,
      CASE WHEN rn = 1 THEN left(slug,63) ELSE left((slug || '_' || rn::text), 63) END AS slug_unique
    FROM base
    ORDER BY question_title
  LOOP
    sql_cols := sql_cols ||
      format('MAX(a.answer_human) FILTER (WHERE a.question_title = %L) AS %I, ',
             rec.question_title, rec.slug_unique);
  END LOOP;

  IF sql_cols = '' THEN
    EXECUTE format($f$
      DROP VIEW IF EXISTS %I.%I CASCADE;
      CREATE VIEW %I.%I AS
      SELECT a.task_id,
             MAX(a.os)  AS os,
             MAX(a.poi) AS poi,
             MAX(co.assignee_name) AS assignee_name,
             MAX(co.assignee_id)   AS assignee_id
      FROM contele.contele_answers a
      LEFT JOIN contele.contele_os co USING (task_id)
      GROUP BY a.task_id;
    $f$, 'contele', view_name, 'contele', view_name);
  ELSE
    sql_cols := left(sql_cols, length(sql_cols)-2);
    EXECUTE format($f$
      DROP VIEW IF EXISTS %I.%I CASCADE;
      CREATE VIEW %I.%I AS
      SELECT
        a.task_id,
        MAX(a.os)  AS os,
        MAX(a.poi) AS poi,
        MAX(co.assignee_name) AS assignee_name,
        MAX(co.assignee_id)   AS assignee_id,
        %s
      FROM contele.contele_answers a
      LEFT JOIN contele.contele_os co USING (task_id)
      GROUP BY a.task_id;
    $f$, 'contele', view_name, 'contele', view_name, sql_cols);
  END IF;
END;
$FN$;

-- Função que (re)cria a view wide de um objetivo específico.
-- Agora inclui assignee_name e assignee_id vindo de contele.contele_os.
-- Faz DROP da view antes de criar para evitar erro de renomeação de coluna.
CREATE OR REPLACE FUNCTION contele.rebuild_view_for_objetivo(objetivo text, view_name text)
RETURNS void
LANGUAGE plpgsql
AS $FN$
DECLARE
  sql_cols text := '';
  rec record;
BEGIN
  FOR rec IN
    WITH distinct_titles AS (
      SELECT DISTINCT a.question_title
      FROM contele.contele_answers a
      WHERE EXISTS (
        SELECT 1
        FROM contele.contele_answers ai
        WHERE ai.task_id = a.task_id
          AND ai.question_title ILIKE 'Qual objetivo%%'
          AND ai.answer_human ILIKE objetivo || '%%'
      )
      AND a.question_title IS NOT NULL
      AND a.question_title NOT ILIKE 'Qual objetivo%%'
    ), base AS (
      SELECT
        question_title,
        contele._slug(question_title) AS slug,
        ROW_NUMBER() OVER (PARTITION BY contele._slug(question_title) ORDER BY question_title) AS rn
      FROM distinct_titles
    )
    SELECT
      question_title,
      CASE WHEN rn = 1 THEN left(slug,63) ELSE left((slug || '_' || rn::text), 63) END AS slug_unique
    FROM base
    ORDER BY question_title
  LOOP
    sql_cols := sql_cols ||
      format('MAX(a.answer_human) FILTER (WHERE a.question_title = %L) AS %I, ',
             rec.question_title, rec.slug_unique);
  END LOOP;

  IF sql_cols = '' THEN
    EXECUTE format($f$
      DROP VIEW IF EXISTS %I.%I CASCADE;
      CREATE VIEW %I.%I AS
      SELECT a.task_id,
             MAX(a.os)  AS os,
             MAX(a.poi) AS poi,
             MAX(co.assignee_name) AS assignee_name,
             MAX(co.assignee_id)   AS assignee_id
      FROM contele.contele_answers a
      LEFT JOIN contele.contele_os co USING (task_id)
      WHERE EXISTS (
        SELECT 1 FROM contele.contele_answers ai
        WHERE ai.task_id = a.task_id
          AND ai.question_title ILIKE 'Qual objetivo%%'
          AND ai.answer_human ILIKE %L
      )
      GROUP BY a.task_id;
    $f$, 'contele', view_name, 'contele', view_name, objetivo || '%');
  ELSE
    sql_cols := left(sql_cols, length(sql_cols)-2);
    EXECUTE format($f$
      DROP VIEW IF EXISTS %I.%I CASCADE;
      CREATE VIEW %I.%I AS
      WITH os_com_obj AS (
        SELECT a.task_id
        FROM contele.contele_answers a
        GROUP BY a.task_id
        HAVING MAX(a.answer_human)
               FILTER (WHERE a.question_title ILIKE 'Qual objetivo%%')
               ILIKE %L
      )
      SELECT
        a.task_id,
        MAX(a.os)  AS os,
        MAX(a.poi) AS poi,
        MAX(co.assignee_name) AS assignee_name,
        MAX(co.assignee_id)   AS assignee_id,
        %s
      FROM contele.contele_answers a
      JOIN os_com_obj oco USING (task_id)
      LEFT JOIN contele.contele_os co USING (task_id)
      GROUP BY a.task_id;
    $f$, 'contele', view_name, 'contele', view_name, objetivo || '%', sql_cols);
  END IF;
END;
$FN$;

-- índice sugerido para acelerar busca/agregação por question_title
CREATE INDEX IF NOT EXISTS idx_contele_answers_question_title_lower ON contele.contele_answers (lower(question_title));
"""

VIEWS_TO_BUILD = [
  ("vw_todas_respostas",),
  ("Prospecção", "vw_prospeccao"),
  ("Relacionamento", "vw_relacionamento"),
  ("Levantamento de Necessidade", "vw_levantamento_de_necessidade"),
  ("Visita Técnica", "vw_visita_tecnica"),
]

def ensure_bootstrap():
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(DDL_BOOTSTRAP)
        for item in VIEWS_TO_BUILD:
            if len(item) == 1:
                view_name = item[0]
                cur.execute("SELECT contele.rebuild_view_all(%s)", (view_name,))
            else:
                objetivo, view_name = item
                cur.execute("SELECT contele.rebuild_view_for_objetivo(%s,%s)", (objetivo, view_name))

def rebuild_views_only():
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        for item in VIEWS_TO_BUILD:
            if len(item) == 1:
                view_name = item[0]
                cur.execute("SELECT contele.rebuild_view_all(%s)", (view_name,))
            else:
                objetivo, view_name = item
                cur.execute("SELECT contele.rebuild_view_for_objetivo(%s,%s)", (objetivo, view_name))

def upsert_os(rows: List[Dict[str,Any]]):
    if not rows:
        return
    rows = dedup_last(rows, ("task_id",), "updated_at")
    sql = """
    INSERT INTO contele.contele_os (
      task_id, os, poi, title, status, assignee_name, assignee_id,
      created_at, finished_at, updated_at, ingested_at, updated_at_local
    ) VALUES %s
    ON CONFLICT (task_id) DO UPDATE SET
      os=EXCLUDED.os, poi=EXCLUDED.poi, title=EXCLUDED.title, status=EXCLUDED.status,
      assignee_name=EXCLUDED.assignee_name, assignee_id=EXCLUDED.assignee_id,
      created_at=EXCLUDED.created_at, finished_at=EXCLUDED.finished_at,
      updated_at=EXCLUDED.updated_at, ingested_at=EXCLUDED.ingested_at, updated_at_local=EXCLUDED.updated_at_local;
    """
    now_iso = now_utc_iso()
    tuples = [(
        r.get("task_id"), r.get("os"), r.get("poi"), r.get("title"), r.get("status"),
        r.get("assignee_name"), r.get("assignee_id"),
        parse_ts(r.get("created_at")), parse_ts(r.get("finished_at")),
        parse_ts(r.get("updated_at")), now_iso, now_iso
    ) for r in rows]
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=1000)
    logging.info(f"Upsert OS: {len(tuples)} linhas.")

def upsert_answers(rows: List[Dict[str,Any]]):
    if not rows:
        return
    rows = dedup_last(rows, ("task_id","form_title","question_id"), "created_at")
    sql = """
    INSERT INTO contele.contele_answers (
      task_id, os, poi, form_title, question_id, question_title,
      answer_human, answer_raw, created_at, ingested_at
    ) VALUES %s
    ON CONFLICT (task_id, question_id) DO UPDATE SET
      os=EXCLUDED.os, poi=EXCLUDED.poi, form_title=EXCLUDED.form_title,
      question_title=EXCLUDED.question_title, answer_human=EXCLUDED.answer_human,
      answer_raw=EXCLUDED.answer_raw, created_at=EXCLUDED.created_at, ingested_at=EXCLUDED.ingested_at;
    """
    now_iso = now_utc_iso()
    tuples = [(
        r.get("task_id"), r.get("os"), r.get("poi"), r.get("form_title"),
        r.get("question_id"), r.get("question_title"),
        r.get("answer_human"), r.get("answer_raw"),
        parse_ts(r.get("created_at")), now_iso
    ) for r in rows]
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, tuples, page_size=2000)
    logging.info(f"Upsert Answers: {len(tuples)} linhas.")

# --------- Contele V2 /tasks + Forms ---------
def iter_tasks(since: str, to: str, tz: str, per_page: int) -> Iterable[Dict[str,Any]]:
    page = 1
    while True:
        params = {"page": page, "perPage": per_page, "sinceDate": since, "toDate": to, "timezone": tz}
        url = f"{V2_BASE}/tasks"
        r = http_get(url, v2_headers(), params)
        if r.status_code in (401,403):
            raise PermissionError(f"V2 /tasks => {r.status_code}")
        r.raise_for_status()
        data = r.json() if r.headers.get("Content-Type","").lower().startswith("application/json") else {}
        items = data.get("items") or data.get("data") or data.get("tasks") or []
        total = data.get("total") or 0

        for item in items:
            task_id = str(item.get("id") or item.get("taskId") or "")
            if not task_id:
                continue

            os_number = str(item.get("os") or item.get("order") or item.get("number") or "")

            poi_name  = ((item.get("poi") or {}).get("name") if isinstance(item.get("poi"), dict)
                         else item.get("poi_name") or item.get("poiName") or "")

            title     = item.get("title") or ""
            status    = item.get("status") or ""

            # -------- responsável / vendedor ----------
            assignee   = item.get("assignee")
            user_data  = item.get("userData") or {}
            user_id    = item.get("userId")

            if isinstance(assignee, dict) and (assignee.get("name") or assignee.get("id")):
                assignee_name = assignee.get("name") or ""
                assignee_id   = assignee.get("id")
            else:
                assignee_name = (
                    user_data.get("name")
                    or item.get("userName")
                    or item.get("creatorName")
                    or item.get("assignee_name")
                    or ""
                )
                assignee_id = (
                    user_data.get("id")
                    or user_id
                    or item.get("assignee_id")
                )

            created_at  = item.get("created_at")  or item.get("createdAt")  or ""
            finished_at = item.get("finished_at") or item.get("finishedAt") or ""
            updated_at  = item.get("updated_at")  or item.get("updatedAt")  or created_at

            yield {
                "task_id": task_id,
                "os": os_number,
                "poi": poi_name,
                "title": title,
                "status": status,
                "assignee_name": assignee_name,
                "assignee_id": str(assignee_id) if assignee_id is not None else "",
                "created_at": created_at,
                "finished_at": finished_at,
                "updated_at": updated_at,
            }

        if total and per_page:
            last_page = int(math.ceil(total / per_page))
            if page >= last_page:
                break
        else:
            if not items or len(items) < per_page:
                break
        page += 1

def list_forms_by_task(task_id: str) -> Dict[str,Any]:
    url = f"{FORMS_BASE}/api/v1/list-forms"
    params = {
        "linked_urns": f"v0:cge:task:{task_id}",
        "page": "1",
        "per_page": "100",
        "add_templates_information_to_form": "true",
        "add_pois_information_to_form": "true",
        "add_tasks_information_to_form": "true",
        "add_users_information_to_form": "true",
        "only_forms_with_answers": "true",
    }
    r = http_get(url, forms_headers(), params)
    r.raise_for_status()
    return r.json()

def build_option_index(form: Dict[str,Any]):
    opt_index: Dict[str, Dict[str,str]] = {}
    title_index: Dict[str,str] = {}
    template = form.get("template") or {}
    form_title = template.get("title") or template.get("name") or ""
    for seg in template.get("segments", []):
        qid = seg.get("id")
        if not qid:
            continue
        title_index[qid] = (seg.get("title") or "").strip()
        options = seg.get("options") or []
        if options:
            opt_index[qid] = {
                opt.get("id"): (opt.get("label") or "").strip()
                for opt in options if opt.get("id")
            }
    return opt_index, title_index, form_title

def humanize_answer(qid: str, raw: Any, opt_index: Dict[str,Dict[str,str]]) -> str:
    if raw is None:
        return ""
    if qid in opt_index:
        parts = [p.strip() for p in str(raw).split(",") if p.strip()]
        labels = [opt_index[qid].get(pid, pid) for pid in parts]
        return ", ".join(labels) if labels else str(raw)
    return str(raw)

def pipeline():
    if not DATABASE_URL:
        raise SystemExit("Defina DATABASE_URL no .env")

    ensure_bootstrap()
    logging.info(f"Período alvo: {SINCE} → {TO} (TZ={TZ})")

    os_rows: List[Dict[str,Any]] = []
    answer_rows: List[Dict[str,Any]] = []

    used_v2 = False
    if V2_BASE and V2_AUTH and V2_KEY:
        try:
            logging.info(f"==> Paginando /tasks (since={SINCE}, to={TO}, tz={TZ}) …")
            for t in iter_tasks(SINCE, TO, TZ, PER_PAGE):
                os_rows.append(t)
                try:
                    data = list_forms_by_task(t["task_id"])
                except requests.HTTPError as e:
                    logging.warning(f"Forms por task {t['task_id']} falhou: {e}")
                    continue
                forms = data.get("forms") or []
                for form in forms:
                    opt_index, title_index, form_title = build_option_index(form)
                    task_meta = (form.get("tasks") or [{}])[0] if form.get("tasks") else {}
                    poi_meta  = (form.get("pois")  or [{}])[0] if form.get("pois")  else {}
                    os_num = str(task_meta.get("os") or t.get("os") or "")
                    poi_nm= str(poi_meta.get("name") or t.get("poi") or "")
                    for ans in form.get("answers", []):
                        qid = ans.get("form_question_id") or ans.get("question_id")
                        raw = ans.get("answer", "")
                        created_at = ans.get("created_at", "")
                        ah = humanize_answer(qid, raw, opt_index)
                        answer_rows.append({
                            "task_id": t["task_id"],
                            "os": os_num,
                            "poi": poi_nm,
                            "form_title": form_title,
                            "question_id": qid,
                            "question_title": title_index.get(qid, f"(Pergunta {qid})"),
                            "answer_human": ah,
                            "answer_raw": raw,
                            "created_at": created_at
                        })
            used_v2 = True
        except PermissionError:
            logging.warning("V2 401/403. Usando apenas Forms (fallback).")

    if not used_v2:
        raise SystemExit("Configure ao menos o Forms (CONTELE_FORMS_BASE/CONTELE_FORMS_BEARER).")

    upsert_os(os_rows)
    upsert_answers(answer_rows)
    logging.info("Ingestão concluída ✔")

    # rebuild views caso novas perguntas tenham surgido
    rebuild_views_only()

if __name__ == "__main__":
    pipeline()