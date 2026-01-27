# -*- coding: utf-8 -*-
import os, re, time, zipfile, requests, logging, traceback, io
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import extensions as _pgext

# ===== tqdm (opcional, mas acelera UX) =====
try:
    from tqdm import tqdm
    TQDM = True
except Exception:
    TQDM = False

# ===== Selenium =====
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

# ==========================
# LOGGING
# ==========================
os.makedirs("logs", exist_ok=True)
LOG_FILE = "logs/etl_run.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ],
)
log = logging.getLogger("safe_road_etl")

# ==========================
# TELEGRAM ALERTS
# ==========================
# ✅ Recomendo manter token/chatid em variáveis de ambiente
TG_TOKEN  = os.getenv("TG_TOKEN",  "Seu Toker Aqui")
TG_CHATID = os.getenv("TG_CHATID", "Seu chat id aqui")
TG_BASE   = f"https://api.telegram.org/bot{TG_TOKEN}"
TG_TIMEOUT = 15

def tg_enabled():
    return TG_TOKEN and TG_CHATID and "COLOQUE_SEU_" not in TG_TOKEN and "COLOQUE_SEU_" not in TG_CHATID

def _tg_post(method, data=None, files=None, retries=2, backoff=1.6):
    if not tg_enabled():
        return {"ok": False, "reason": "telegram_not_configured"}
    data = data or {}
    for i in range(retries + 1):
        try:
            r = requests.post(f"{TG_BASE}/{method}", data=data, files=files, timeout=TG_TIMEOUT)
            try:
                return r.json()
            except Exception:
                return {"ok": False, "http": r.status_code, "text": r.text[:300]}
        except Exception as e:
            if i == retries:
                return {"ok": False, "error": str(e)}
            time.sleep(backoff ** i)

def tg_send_message(text, parse_mode=None, disable_web_page_preview=True):
    data = {"chat_id": str(TG_CHATID), "text": text}
    if parse_mode:
        data["parse_mode"] = parse_mode
    if disable_web_page_preview:
        data["disable_web_page_preview"] = True
    return _tg_post("sendMessage", data)

def tg_send_document_from_path(path, caption=None):
    if not os.path.isfile(path):
        return {"ok": False, "reason": "file_not_found"}
    with open(path, "rb") as fp:
        files = {"document": (os.path.basename(path), fp)}
        data  = {"chat_id": str(TG_CHATID)}
        if caption:
            data["caption"] = caption[:1024]
        return _tg_post("sendDocument", data=data, files=files)

def tg_send_document_bytes(filename, content_bytes, caption=None):
    files = {"document": (filename, io.BytesIO(content_bytes))}
    data  = {"chat_id": str(TG_CHATID)}
    if caption:
        data["caption"] = caption[:1024]
    return _tg_post("sendDocument", data=data, files=files)

def fmt_sec(s):
    try:
        s = float(s)
        m, s = divmod(int(s), 60)
        h, m = divmod(m, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return "-"

def tg_alert_success(stats, tempos):
    linhas  = stats.get("fact_inserted_rows", 0)
    lotes   = stats.get("fact_batches", 0)
    skipped = stats.get("fact_skipped_null_keys", 0)
    msg = (
        "✅ *ETL BI SafeRoad 2.0 finalizado com sucesso.*\n"
        f"*Fato inserido:* `{linhas:,}` linhas em `{lotes}` lote(s)\n"
        f"*Puladas por chave nula:* `{skipped}`\n\n"
        "*Duração das etapas:*\n"
        f"- Extract: `{fmt_sec(tempos.get('extract', 0))}`\n"
        f"- Transform: `{fmt_sec(tempos.get('transform', 0))}`\n"
        f"- Load: `{fmt_sec(tempos.get('load', 0))}`\n"
    )
    tg_send_message(msg, parse_mode="Markdown")
    if os.path.isfile(LOG_FILE):
        tg_send_document_from_path(LOG_FILE, caption="📎 Log da execução")

def tg_alert_error(etapa, err, started_at=None):
    tb = "".join(traceback.format_exception_only(type(err), err)).strip()
    if hasattr(err, "__traceback__"):
        tb_full = "".join(traceback.format_tb(err.__traceback__))
    else:
        tb_full = ""
    msg = (
        "❌ *ETL PRF falhou*\n"
        f"*Etapa:* `{etapa}`\n"
        f"*Erro:* `{tb}`\n"
    )
    if started_at:
        dur = (datetime.now() - started_at).total_seconds()
        msg += f"*Tempo até o erro:* `{fmt_sec(dur)}`\n"
    short_tb = tb_full[-1500:] if len(tb_full) > 1500 else tb_full
    if short_tb:
        msg += "\n*Traceback (resumo):*\n```\n" + short_tb + "\n```"
    tg_send_message(msg, parse_mode="Markdown")
    if os.path.isfile(LOG_FILE):
        tg_send_document_from_path(LOG_FILE, caption=f"📎 Log - erro na etapa {etapa}")

# ==========================
# CONFIGURAÇÕES
# ==========================
SITE_PRF = "https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf"
EXTRACT_FOLDER = r"C:\Users\umble\OneDrive\Área de Trabalho\Datatran Novo\Bases"
os.makedirs(EXTRACT_FOLDER, exist_ok=True)
CSV_OUTPUT = os.path.join(EXTRACT_FOLDER, "acidentes_consolidados.csv")

DB_CONFIG = {
    'dbname': 'dw_datatran',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

ANOS = ["2025","2024","2023","2022","2021","2020","2019"]
TITULO_DATASET = "Agrupados por pessoa - Todas as causas e tipos de acidentes"

# Otimização
BATCH = 20000
PAGE_SIZE = 5000

# ==========================
# DOWNLOAD
# ==========================
def build_driver(headless=True):
    opts = Options()
    opts.page_load_strategy = "eager"
    if headless or os.getenv("HEADLESS","1")=="1":
        opts.add_argument("--headless")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-software-rasterizer")
    prefs = {"profile.managed_default_content_settings.images":2,
             "profile.managed_default_content_settings.fonts":2,
             "profile.managed_default_content_settings.stylesheets":1}
    opts.add_experimental_option("prefs", prefs)
    d = webdriver.Chrome(service=Service(), options=opts)
    d.set_page_load_timeout(45)
    return d

def safe_get(d,url,retries=3,backoff=1.5):
    for i in range(retries):
        try:
            d.get(url)
            return
        except Exception:
            if i==retries-1:
                raise
            time.sleep(backoff*(i+1))

def safe_click_accept_cookies(d,timeout=10):
    cands=[(By.XPATH,'//button[contains(., "Aceitar") or contains(., "Entendi") or contains(., "OK")]'),
           (By.XPATH,'/html/body//button[contains(., "Aceitar") or contains(., "Entendi")]'),
           (By.CSS_SELECTOR,'button[title*="cookie"], button[id*="cookie"], .cookie-modal button')]
    for by,sel in cands:
        try:
            btn=WebDriverWait(d,timeout).until(EC.element_to_be_clickable((by,sel)))
            btn.click(); time.sleep(0.2); return True
        except Exception:
            continue
    return False

def get_link_for_year(d,ano,timeout=30):
    WebDriverWait(d,timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR,"#parent-fieldname-text")))
    def ci(expr):
        UP="ABCDEFGHIJKLMNOPQRSTUVWXYZÁÂÃÀÉÊÍÓÔÕÚÇ"; LO="abcdefghijklmnopqrstuvwxyzáâãàéêíóôõúç"
        return f"translate({expr}, '{UP}', '{LO}')"
    titulo_low = TITULO_DATASET.lower()
    xp_tr=('//*[@id="parent-fieldname-text"]//tr['
           f'.//td[contains({ci("normalize-space(.)")}, "{titulo_low}")] and .//text()[contains(., "{ano}")]'
           ']//a[contains(@href,"drive.google")][1]')
    xp_block=('//*[@id="parent-fieldname-text"]//*[self::p or self::div or self::li]['
             f'contains({ci("normalize-space(.)")}, "{titulo_low}") and contains(normalize-space(.), "{ano}")]'
             '//a[contains(@href,"drive.google")][1]')
    xp_sibling=('//*[@id="parent-fieldname-text"]//*[contains('
               f'{ci("normalize-space(.)")}, "{titulo_low}")'
               ')]//a[contains(@href,"drive.google") and contains(normalize-space(following-sibling::text()[1]), '
               f'"{ano}")][1]')
    for xp in (xp_tr,xp_block,xp_sibling):
        try:
            el=WebDriverWait(d,10).until(EC.presence_of_element_located((By.XPATH,xp)))
            href=el.get_attribute("href") or ""
            if href: return href
        except Exception:
            continue
    raise NoSuchElementException(f"Link de {ano} (dataset correto) não encontrado.")

def extract_drive_file_id(url):
    m=re.search(r"/d/([a-zA-Z0-9_-]+)",url);  m=m or re.search(r"[?&]id=([a-zA-Z0-9_-]+)",url)
    return m.group(1) if m else None

def download_from_drive(file_id,dest_zip,chunk=1024*1024):
    s=requests.Session(); URL="https://drive.google.com/uc?export=download"
    def _tok(r):
        for k,v in r.cookies.items():
            if k.startswith("download_warning"): return v
    p={"id":file_id}; r=s.get(URL,params=p,stream=True); r.raise_for_status()
    t=_tok(r)
    if t: p["confirm"]=t; r=s.get(URL,params=p,stream=True); r.raise_for_status()
    with open(dest_zip,"wb") as f:
        for c in r.iter_content(chunk_size=chunk):
            if c: f.write(c)

# ==========================
# DB & NORMALIZAÇÃO
# ==========================
def connect_pg(cfg):
    os.environ.setdefault("PGCLIENTENCODING","UTF8")
    dsn=_pgext.make_dsn(dbname=str(cfg['dbname']), user=str(cfg['user']), password=str(cfg['password']),
                        host=str(cfg['host']), port=int(str(cfg['port']).strip()))
    conn=psycopg2.connect(dsn)
    conn.set_client_encoding('UTF8')
    return conn

def as_text(x, default="NÃO INFORMADO"):
    if pd.isna(x) or x is None or str(x).strip()=="": return default
    return str(x).strip()

def as_int(x, default=0):
    try: return int(pd.to_numeric(x, errors='coerce'))
    except Exception: return default

def as_float(x, default=0.0):
    try: return float(pd.to_numeric(x, errors='coerce'))
    except Exception: return default

def as_date(x):
    try: return pd.to_datetime(x, errors="coerce").date()
    except Exception: return None

def as_time(x):
    if isinstance(x,str):
        try: return pd.to_datetime(x, format="%H:%M:%S", errors="raise").time()
        except Exception: pass
    try: return pd.to_datetime(str(x), errors="coerce").time()
    except Exception: return None

# ==========================
# AUDITORIA DA CARGA
# ==========================
def ensure_etl_log_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id SERIAL PRIMARY KEY,
            etapa VARCHAR(100),
            registros_processados INT,
            inicio TIMESTAMP,
            fim TIMESTAMP,
            duracao_segundos NUMERIC(10,2),
            status VARCHAR(20),
            erro TEXT
        );
    """)

def insert_etl_log(cur, etapa, inicio, fim, registros, status="OK", erro=None):
    dur = (fim - inicio).total_seconds() if inicio and fim else None
    cur.execute("""
        INSERT INTO etl_log (etapa, registros_processados, inicio, fim, duracao_segundos, status, erro)
        VALUES (%s,%s,%s,%s,%s,%s,%s);
    """, (etapa, int(registros or 0), inicio, fim, dur, status, erro))

# ==========================
# 1) CAPTURA + CONSOLIDAÇÃO
# ==========================
tempos = {}
extract_ini = datetime.now()
log.info("Iniciando etapa EXTRACT")
reg_extract = 0

try:
    driver=build_driver(headless=True); todos=[]
    try:
        safe_get(driver,SITE_PRF); safe_click_accept_cookies(driver)
        anos_iter = tqdm(ANOS, desc="Anos") if TQDM else ANOS
        for ano in anos_iter:
            log.info(f"Baixando ano {ano}…")
            try:
                href=get_link_for_year(driver,ano,timeout=35)
                fid=extract_drive_file_id(href)
                if not fid:
                    log.warning(f"Sem file_id para {ano}. Pulando.")
                    continue
                zip_name=os.path.join(EXTRACT_FOLDER,f"base_acidentes_{ano}.zip")
                download_from_drive(fid,zip_name)
                extract_path=os.path.join(EXTRACT_FOLDER,f"acidentes_{ano}")
                os.makedirs(extract_path,exist_ok=True)
                with zipfile.ZipFile(zip_name,'r') as z: z.extractall(extract_path)
                for fn in os.listdir(extract_path):
                    if fn.lower().endswith(".csv"):
                        df_tmp=pd.read_csv(os.path.join(extract_path,fn), sep=';', encoding='latin1', low_memory=False)
                        df_tmp["ANO"]=int(ano)
                        reg_extract += len(df_tmp)
                        todos.append(df_tmp)
                        log.info(f"CSV lido: {fn} ({len(df_tmp):,} linhas)")
            except Exception as e:
                log.exception(f"Erro no ano {ano}: {e}")
    finally:
        try: driver.quit()
        except: pass

    if not todos:
        raise RuntimeError("Nenhum dado consolidado na extração.")

    df=pd.concat(todos, ignore_index=True)
    extract_end = datetime.now()
    tempos["extract"] = (extract_end - extract_ini).total_seconds()
    log.info(f"EXTRACT concluído com {len(df):,} linhas.")
except Exception as e:
    extract_end = datetime.now()
    tempos["extract"] = (extract_end - extract_ini).total_seconds()
    log.exception("Falha na etapa EXTRACT")
    conn = connect_pg(DB_CONFIG); conn.autocommit=True
    cur = conn.cursor()
    ensure_etl_log_table(cur)
    insert_etl_log(cur, "EXTRACT", extract_ini, extract_end, reg_extract, status="ERRO", erro=str(e))
    cur.close(); conn.close()
    tg_alert_error("EXTRACT", e, extract_ini)
    raise

# auditoria extract (OK)
conn = connect_pg(DB_CONFIG); conn.autocommit=True
cur = conn.cursor()
ensure_etl_log_table(cur)
insert_etl_log(cur, "EXTRACT", extract_ini, extract_end, reg_extract, status="OK", erro=None)
cur.close(); conn.close()

# ==========================
# 2) TRATAMENTOS
# ==========================
transform_ini = datetime.now()
log.info("Iniciando etapa TRANSFORM")
try:
    if 'ano_fabricacao_veiculo' in df.columns: df.rename(columns={'ano_fabricacao_veiculo':'ano_fabricacao'}, inplace=True)
    if 'ID' in df.columns: df.rename(columns={'ID':'id_ac'}, inplace=True)

    # Condição meteorológica
    cand_cnd=['condicao_meteorologica','condicao_metereologica','cond_meteorologica','cond_meteo','condicao_tempo']
    lower_map={c.lower():c for c in df.columns}
    cnd_col = next((c for c in cand_cnd if c in lower_map), None)
    if cnd_col:
        df.rename(columns={lower_map[cnd_col]:'condicao_meteorologica'}, inplace=True)
    else:
        df['condicao_meteorologica']=None
    df['condicao_meteorologica']=(df['condicao_meteorologica'].astype(str).str.strip().str.slice(0,100)
                                    .replace({'':"NÃO INFORMADO","None":"NÃO INFORMADO"}))

    # Numéricos
    for col in ['idade','ilesos','feridos_leves','feridos_graves','mortos','pesid','br']:
        if col in df.columns: df[col]=pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    # KM e coordenadas
    if 'km' in df.columns:
        df['km']=(df['km'].astype(str).str.replace(",",".",regex=False).str.extract(r'([-+]?\d*\.?\d+)', expand=False))
        df['km']=pd.to_numeric(df['km'], errors='coerce').fillna(0.0)

    for col in ['latitude','longitude']:
        if col in df.columns:
            df[col]=pd.to_numeric(df[col].astype(str).str.replace(",",".",regex=False), errors='coerce').fillna(0.0)

    # Textos padrão
    for col in ['tipo_veiculo','tipo_envolvido','estado_fisico','sexo','marca',
                'tipo_acidente','classificacao_acidente','municipio','uf',
                'sentido_via','tipo_pista','tracado_via','uso_solo','condicao_meteorologica']:
        if col in df.columns: df[col]=df[col].fillna("NÃO INFORMADO").replace('',"NÃO INFORMADO")

    # ✅ Padronização: "Automóvel" -> "Carro de Passeio"
    if 'tipo_veiculo' in df.columns:
        df['tipo_veiculo'] = (
            df['tipo_veiculo']
              .astype(str)
              .str.replace(r'(?i)\bautom[oó]vel\b', 'Carro de Passeio', regex=True)
        )

    # Ano de fabricação
    if 'ano_fabricacao' in df.columns:
        df['ano_fabricacao']=pd.to_numeric(df['ano_fabricacao'], errors='coerce').fillna(1900).astype(int)
        df.loc[df['ano_fabricacao']<=0,'ano_fabricacao']=1900

    # Datas/tempo
    if 'data_inversa' in df.columns:
        df['data_completa']=pd.to_datetime(df['data_inversa'], errors='coerce')
    else:
        possiveis=[c for c in df.columns if 'data' in c.lower()]
        df['data_completa']=pd.to_datetime(possiveis and df[possiveis[0]] or pd.NaT, errors='coerce')

    df['horario_dt']=pd.to_datetime(df['horario'], errors='coerce') if 'horario' in df.columns else pd.NaT

    df['ano']=df['data_completa'].dt.year.fillna(1900).astype('Int64')
    df['mes']=df['data_completa'].dt.month.fillna(0).astype('Int64')
    df['dia']=df['data_completa'].dt.day.fillna(0).astype('Int64')
    df['trimestre']=df['data_completa'].dt.quarter.fillna(0).astype('Int64')

    # ==========================
    # ✅ (NOVO) Mês e dia da semana em PT-BR + campos de ordenação
    # ==========================
    MESES_PT = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    DIAS_SEMANA_PT = {
        0: "Segunda-feira",
        1: "Terça-feira",
        2: "Quarta-feira",
        3: "Quinta-feira",
        4: "Sexta-feira",
        5: "Sábado",
        6: "Domingo"
    }

    # Ordenação (recomendado no PBI: classificar nome_mes por mes_ord; dia_semana por dia_semana_ord)
    df['mes_ord'] = df['data_completa'].dt.month.fillna(0).astype('Int64')
    # 1..7 (Segunda=1 ... Domingo=7)
    df['dia_semana_ord'] = (df['data_completa'].dt.dayofweek + 1).fillna(0).astype('Int64')

    df['nome_mes'] = df['data_completa'].dt.month.map(MESES_PT).fillna('NÃO INFORMADO')
    df['dia_semana'] = df['data_completa'].dt.dayofweek.map(DIAS_SEMANA_PT).fillna('NÃO INFORMADO')

    # fase do dia
    if 'fase_dia' not in df.columns:
        horas=df['horario_dt'].dt.hour
        df['fase_dia']=pd.cut(horas, bins=[-1,5,11,17,23],
                              labels=['MADRUGADA','MANHÃ','TARDE','NOITE']).astype(str).fillna('NÃO INFORMADO')

    # === (NOVO) Causa do acidente ===
    cand_causa = ['causa_acidente','causa','causa_principal','descricao_causa','motivo_acidente']
    lower_map  = {c.lower(): c for c in df.columns}
    src        = next((c for c in cand_causa if c in lower_map), None)

    if src:
        df.rename(columns={lower_map[src]: 'causa_acidente'}, inplace=True)
    if 'causa_acidente' not in df.columns:
        df['causa_acidente'] = None

    df['causa_acidente'] = (
        df['causa_acidente']
            .astype(str).str.strip()
            .replace({'': 'NÃO INFORMADO', 'None': 'NÃO INFORMADO'})
            .str.slice(0, 255)
    )

    # CSV para inspeção
    df_out=df.copy(); df_out['horario']=df['horario_dt'].dt.time
    df_out.to_csv(CSV_OUTPUT, sep=';', index=False, encoding='latin1')
    log.info(f"TRANSFORM concluído. CSV: {CSV_OUTPUT}")
    transform_end = datetime.now()
    tempos["transform"] = (transform_end - transform_ini).total_seconds()
except Exception as e:
    transform_end = datetime.now()
    tempos["transform"] = (transform_end - transform_ini).total_seconds()
    log.exception("Falha na etapa TRANSFORM")
    conn = connect_pg(DB_CONFIG); conn.autocommit=True
    cur = conn.cursor(); ensure_etl_log_table(cur)
    insert_etl_log(cur, "TRANSFORM", transform_ini, transform_end, len(df) if 'df' in locals() else 0, status="ERRO", erro=str(e))
    cur.close(); conn.close()
    tg_alert_error("TRANSFORM", e, transform_ini)
    raise

# auditoria transform (OK)
conn = connect_pg(DB_CONFIG); conn.autocommit=True
cur = conn.cursor(); ensure_etl_log_table(cur)
insert_etl_log(cur, "TRANSFORM", transform_ini, transform_end, len(df), status="OK", erro=None)
cur.close(); conn.close()

# ==========================
# 3) CARGA (get_or_create + batches)
# ==========================
load_ini = datetime.now()
log.info("Iniciando etapa LOAD")

conn=connect_pg(DB_CONFIG)
cur=conn.cursor()

# Otimizações
cur.execute("SET synchronous_commit = OFF;")
cur.execute("SET temp_buffers = '128MB';")
cur.execute("SET work_mem = '256MB';")
conn.commit()

def ensure_etl_structures(cur, conn):
    ensure_etl_log_table(cur)

    # ✅ Garantir colunas de ordenação na dim_tempo (sem quebrar quem já tem a tabela)
    cur.execute("ALTER TABLE IF EXISTS dim_tempo ADD COLUMN IF NOT EXISTS mes_ord INT;")
    cur.execute("ALTER TABLE IF EXISTS dim_tempo ADD COLUMN IF NOT EXISTS dia_semana_ord INT;")
    conn.commit()

ensure_etl_structures(cur, conn)

def sql_truncate_all(cur,conn):
    cur.execute("""
        TRUNCATE TABLE
            fato_acidentes,
            dim_tempo,
            dim_vitima,
            dim_localidade,
            dim_veiculo,
            dim_pista,
            dim_acidente,
            dim_cnd_meteorologica
        RESTART IDENTITY
        CASCADE;
    """); conn.commit()

# Limpeza para carga full
sql_truncate_all(cur,conn)

# ---- contadores/telemetria ----
stats={k:{'inserted':0,'lookups':0} for k in
       ['dim_acidente','dim_pista','dim_veiculo','dim_localidade','dim_vitima','dim_tempo','dim_cnd_meteorologica']}
stats.update({'fact_inserted_rows':0,'fact_batches':0,'fact_skipped_null_keys':0})
_cache={k:{} for k in stats if k.startswith('dim_')}

# ---- funções get_or_create com contagem ----
def get_or_create_dim_acidente(r):
    tipo = as_text(r.get('tipo_acidente'))
    cls  = as_text(r.get('classificacao_acidente'))
    csa  = as_text(r.get('causa_acidente'))

    key = (tipo, cls, csa)
    if key in _cache['dim_acidente']:
        stats['dim_acidente']['lookups']+=1
        return _cache['dim_acidente'][key]

    cur.execute("""
        SELECT id_acidente FROM dim_acidente
        WHERE tipo_acidente=%s AND classificacao_acidente=%s AND causa_acidente=%s
        ORDER BY id_acidente DESC LIMIT 1
    """, key)
    t=cur.fetchone()
    if t:
        _cache['dim_acidente'][key]=t[0]
        stats['dim_acidente']['lookups']+=1
        return t[0]

    cur.execute("""
        INSERT INTO dim_acidente (tipo_acidente, classificacao_acidente, causa_acidente)
        VALUES (%s,%s,%s) RETURNING id_acidente
    """, key)
    nid=cur.fetchone()[0]
    _cache['dim_acidente'][key]=nid
    stats['dim_acidente']['inserted']+=1
    return nid

def get_or_create_dim_pista(r):
    key=(as_text(r.get('sentido_via')), as_text(r.get('tipo_pista')),
         as_text(r.get('tracado_via')), as_text(r.get('uso_solo')))
    if key in _cache['dim_pista']: stats['dim_pista']['lookups']+=1; return _cache['dim_pista'][key]
    cur.execute("""SELECT id_pista FROM dim_pista
                   WHERE sentido_via=%s AND tipo_pista=%s AND tracado_via=%s AND uso_solo=%s
                   ORDER BY id_pista DESC LIMIT 1""", key)
    t=cur.fetchone()
    if t: _cache['dim_pista'][key]=t[0]; stats['dim_pista']['lookups']+=1; return t[0]
    cur.execute("""INSERT INTO dim_pista (sentido_via, tipo_pista, tracado_via, uso_solo)
                   VALUES (%s,%s,%s,%s) RETURNING id_pista""", key)
    nid=cur.fetchone()[0]; _cache['dim_pista'][key]=nid; stats['dim_pista']['inserted']+=1; return nid

def get_or_create_dim_veiculo(r):
    key=(as_text(r.get('tipo_veiculo')), as_text(r.get('marca')), as_int(r.get('ano_fabricacao'),1900))
    if key in _cache['dim_veiculo']: stats['dim_veiculo']['lookups']+=1; return _cache['dim_veiculo'][key]
    cur.execute("""SELECT id_veiculo FROM dim_veiculo
                   WHERE tipo_veiculo=%s AND marca=%s AND ano_fabricacao=%s
                   ORDER BY id_veiculo DESC LIMIT 1""", key)
    t=cur.fetchone()
    if t: _cache['dim_veiculo'][key]=t[0]; stats['dim_veiculo']['lookups']+=1; return t[0]
    cur.execute("""INSERT INTO dim_veiculo (tipo_veiculo, marca, ano_fabricacao)
                   VALUES (%s,%s,%s) RETURNING id_veiculo""", key)
    nid=cur.fetchone()[0]; _cache['dim_veiculo'][key]=nid; stats['dim_veiculo']['inserted']+=1; return nid

def get_or_create_dim_localidade(r):
    key=(as_text(r.get('municipio')), as_text(r.get('uf')), as_int(r.get('br'),0),
         round(as_float(r.get('km'),0.0),2), round(as_float(r.get('latitude'),0.0),6),
         round(as_float(r.get('longitude'),0.0),6))
    if key in _cache['dim_localidade']: stats['dim_localidade']['lookups']+=1; return _cache['dim_localidade'][key]
    cur.execute("""SELECT id_localidade FROM dim_localidade
                   WHERE municipio=%s AND uf=%s AND br=%s AND km=%s AND latitude=%s AND longitude=%s
                   ORDER BY id_localidade DESC LIMIT 1""", key)
    t=cur.fetchone()
    if t: _cache['dim_localidade'][key]=t[0]; stats['dim_localidade']['lookups']+=1; return t[0]
    cur.execute("""INSERT INTO dim_localidade (municipio, uf, br, km, latitude, longitude)
                   VALUES (%s,%s,%s,%s,%s,%s) RETURNING id_localidade""", key)
    nid=cur.fetchone()[0]; _cache['dim_localidade'][key]=nid; stats['dim_localidade']['inserted']+=1; return nid

def get_or_create_dim_vitima(r):
    key=(as_text(r.get('sexo')), as_int(r.get('idade'),0),
         as_text(r.get('estado_fisico')), as_text(r.get('tipo_envolvido')))
    if key in _cache['dim_vitima']: stats['dim_vitima']['lookups']+=1; return _cache['dim_vitima'][key]
    cur.execute("""SELECT id_vitima FROM dim_vitima
                   WHERE sexo=%s AND idade=%s AND estado_fisico=%s AND tipo_envolvido=%s
                   ORDER BY id_vitima DESC LIMIT 1""", key)
    t=cur.fetchone()
    if t: _cache['dim_vitima'][key]=t[0]; stats['dim_vitima']['lookups']+=1; return t[0]
    cur.execute("""INSERT INTO dim_vitima (sexo, idade, estado_fisico, tipo_envolvido)
                   VALUES (%s,%s,%s,%s) RETURNING id_vitima""", key)
    nid=cur.fetchone()[0]; _cache['dim_vitima'][key]=nid; stats['dim_vitima']['inserted']+=1; return nid

def get_or_create_dim_tempo(r):
    key=(
        as_date(r.get('data_completa')),
        as_time(r.get('horario_dt')),
        as_text(r.get('fase_dia')),
        as_int(r.get('ano'),1900),
        as_int(r.get('mes'),0),
        as_int(r.get('dia'),0),
        as_int(r.get('trimestre'),0),
        as_text(r.get('nome_mes')),
        as_text(r.get('dia_semana')),
        as_int(r.get('mes_ord'),0),
        as_int(r.get('dia_semana_ord'),0),
    )
    if key in _cache['dim_tempo']:
        stats['dim_tempo']['lookups']+=1
        return _cache['dim_tempo'][key]

    cur.execute("""
        SELECT id_tempo FROM dim_tempo
        WHERE data_completa=%s AND horario=%s AND fase_dia=%s AND
              ano=%s AND mes=%s AND dia=%s AND trimestre=%s AND
              nome_mes=%s AND dia_semana=%s AND mes_ord=%s AND dia_semana_ord=%s
        ORDER BY id_tempo DESC LIMIT 1
    """, key)
    t=cur.fetchone()
    if t:
        _cache['dim_tempo'][key]=t[0]
        stats['dim_tempo']['lookups']+=1
        return t[0]

    cur.execute("""
        INSERT INTO dim_tempo
          (data_completa, horario, fase_dia, ano, mes, dia, trimestre, nome_mes, dia_semana, mes_ord, dia_semana_ord)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id_tempo
    """, key)
    nid=cur.fetchone()[0]
    _cache['dim_tempo'][key]=nid
    stats['dim_tempo']['inserted']+=1
    return nid

def get_or_create_dim_cnd(r):
    cnd=as_text(r.get('condicao_meteorologica'))
    key=(cnd,)
    if key in _cache['dim_cnd_meteorologica']: stats['dim_cnd_meteorologica']['lookups']+=1; return _cache['dim_cnd_meteorologica'][key]
    cur.execute("""SELECT id_cnd FROM dim_cnd_meteorologica
                   WHERE cnd_meteorologica=%s ORDER BY id_cnd DESC LIMIT 1""", key)
    t=cur.fetchone()
    if t: _cache['dim_cnd_meteorologica'][key]=t[0]; stats['dim_cnd_meteorologica']['lookups']+=1; return t[0]
    cur.execute("""INSERT INTO dim_cnd_meteorologica (cnd_meteorologica)
                   VALUES (%s) RETURNING id_cnd""", key)
    nid=cur.fetchone()[0]; _cache['dim_cnd_meteorologica'][key]=nid; stats['dim_cnd_meteorologica']['inserted']+=1; return nid

# ---- FATO ----
buffer=[]; total=len(df)
log.info(f"Inserindo FATO (linhas: {total:,})…")

iter_rows = tqdm(df.iterrows(), total=total, desc="LOAD") if TQDM else df.iterrows()
try:
    for i, row in iter_rows:
        id_acidente = get_or_create_dim_acidente(row)
        id_pista    = get_or_create_dim_pista(row)
        id_veiculo  = get_or_create_dim_veiculo(row)
        id_local    = get_or_create_dim_localidade(row)
        id_vitima   = get_or_create_dim_vitima(row)
        id_tempo    = get_or_create_dim_tempo(row)
        id_cnd      = get_or_create_dim_cnd(row)

        if None in (id_acidente,id_pista,id_veiculo,id_local,id_vitima,id_tempo,id_cnd):
            stats['fact_skipped_null_keys']+=1; continue

        ilesos=as_int(row.get('ilesos'),0)
        fl=as_int(row.get('feridos_leves'),0)
        fg=as_int(row.get('feridos_graves'),0)
        m=as_int(row.get('mortos'),0)

        buffer.append((id_tempo,id_vitima,id_pista,id_acidente,id_veiculo,id_local,id_cnd,ilesos,fl,fg,m))

        if len(buffer)>=BATCH:
            execute_values(cur, """
                INSERT INTO fato_acidentes
                  (id_tempo, id_vitima, id_pista, id_acidente, id_veiculo, id_localidade, id_cnd,
                   ilesos, feridos_leves, feridos_graves, mortos)
                VALUES %s
            """, buffer, page_size=PAGE_SIZE)
            conn.commit()
            stats['fact_inserted_rows']+=len(buffer); stats['fact_batches']+=1; buffer.clear()

        if (i+1)%50000==0:
            log.info(f"   • Processadas {i+1:,}/{total:,} (fato inseridas: {stats['fact_inserted_rows']:,})")

    if buffer:
        execute_values(cur, """
            INSERT INTO fato_acidentes
              (id_tempo, id_vitima, id_pista, id_acidente, id_veiculo, id_localidade, id_cnd,
               ilesos, feridos_leves, feridos_graves, mortos)
            VALUES %s
        """, buffer, page_size=PAGE_SIZE)
        conn.commit()
        stats['fact_inserted_rows']+=len(buffer); stats['fact_batches']+=1; buffer.clear()

    load_end = datetime.now()
    tempos["load"] = (load_end - load_ini).total_seconds()
    log.info("LOAD concluído com sucesso.")

except Exception as e:
    load_end = datetime.now()
    tempos["load"] = (load_end - load_ini).total_seconds()
    log.exception("Falha na etapa LOAD")

    # limpar estado de transação abortada
    try:
        conn.rollback()
    except Exception:
        pass

    ensure_etl_structures(cur, conn)
    insert_etl_log(cur, "LOAD", load_ini, load_end, stats.get('fact_inserted_rows',0), status="ERRO", erro=str(e))
    conn.commit()
    tg_alert_error("LOAD", e, load_ini)
    cur.close(); conn.close()
    raise

# auditoria load (OK)
ensure_etl_structures(cur, conn)
insert_etl_log(cur, "LOAD", load_ini, load_end, stats['fact_inserted_rows'], status="OK", erro=None)
conn.commit()

cur.close(); conn.close()

# ==========================
# 4) RESUMO
# ==========================
log.info("RESUMO DA CARGA")
for dim in ['dim_acidente','dim_pista','dim_veiculo','dim_localidade','dim_vitima','dim_tempo','dim_cnd_meteorologica']:
    log.info(f"{dim}: {stats[dim]['inserted']} novas chaves, {stats[dim]['lookups']} lookups")
log.info(f"fato_acidentes: {stats['fact_inserted_rows']:,} linhas em {stats['fact_batches']} lote(s)")
log.info(f"linhas puladas por chave nula: {stats.get('fact_skipped_null_keys',0)}")
log.info("✅ Pipeline finalizada com sucesso.")

# Envia alerta de sucesso (Telegram)
try:
    tg_alert_success(stats, tempos)
except Exception as _e:
    log.warning(f"Falha ao enviar alerta de sucesso no Telegram: {_e}")
