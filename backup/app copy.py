import os, json, time, logging, datetime, threading, socket
from pathlib import Path

import requests, pyodbc
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, ROUND_HALF_UP

# ---------------- Carga .env ----------------
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(ENV_PATH, override=False)
print(f"[ENV] Cargando .env desde: {ENV_PATH} (existe={ENV_PATH.exists()})")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---------------- Config -----------------
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_PORT     = os.getenv("SQL_PORT", "1433")
SQL_DB       = os.getenv("SQL_DB")
SQL_USER     = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER   = os.getenv("SQL_DRIVER")  # opcional

CLIENT_ID     = os.getenv("SUNAT_CLIENT_ID")
CLIENT_SECRET = os.getenv("SUNAT_CLIENT_SECRET")
RUC_CONTRIB   = os.getenv("SUNAT_RUC")  # RUC receptor en URL

WORKER_BATCH   = int(os.getenv("WORKER_BATCH", "300"))
WORKER_THREADS = int(os.getenv("WORKER_THREADS", "10"))
RETRY_MAX      = int(os.getenv("RETRY_MAX", "3"))
HTTP_TIMEOUT   = int(os.getenv("HTTP_TIMEOUT", "25"))

# API de validación
VALIDA_URL = f"https://api.sunat.gob.pe/v1/contribuyente/contribuyentes/{RUC_CONTRIB}/validarcomprobante"

# Tablas
T_QUEUE     = "INH.API_SUNAT_QUEUE"
T_HIST      = "INH.SUNAT_VALIDACION"
T_SNAPSHOT  = "INH.SUNAT_ESTADO_ACTUAL"
T_FINAL     = "DATA.FACTURA_COMPRA_BACKUS_CABECERA"  # columnas SUNAT_* aquí

# -------------- Token cache --------------
_token_lock   = threading.Lock()
_cached_token = None
_token_exp    = datetime.datetime.now(datetime.timezone.utc)

# -------------- SQL helpers --------------
def _pick_sql_driver():
    prefs = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    installed = [d.strip() for d in pyodbc.drivers()]
    for p in prefs:
        if p in installed:
            return p
    raise RuntimeError(f"No se encontró un driver ODBC 17/18. Detectados: {installed}")

def _effective_driver():
    wanted = os.getenv("SQL_DRIVER")
    installed = [d.strip() for d in pyodbc.drivers()]
    if wanted and wanted in installed:
        return wanted
    if wanted and wanted not in installed:
        logging.warning(f"SQL_DRIVER='{wanted}' no encontrado. Usando driver disponible: {installed}")
    return _pick_sql_driver()

def _require_env(name, default=None):
    val = os.getenv(name, default)
    if val is None or str(val).strip() == "":
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return str(val).strip()

def _server_part(host, port):
    if "\\" in host:
        return host
    return f"{host},{port}" if port else host

def _diagnose_dns(host):
    try:
        socket.inet_aton(host)  # IPv4 literal
        return True
    except OSError:
        pass
    try:
        socket.getaddrinfo(host.split("\\")[0], None)
        return True
    except Exception as e:
        logging.error(f"No se puede resolver '{host}': {e}")
        return False

def sql_cnx():
    driver  = _effective_driver()
    host    = _require_env("SQL_SERVER")
    port    = os.getenv("SQL_PORT", "1433").strip()
    db      = _require_env("SQL_DB")
    user    = _require_env("SQL_USER")
    pwd     = _require_env("SQL_PASSWORD")

    if "\\" not in host:
        try:
            socket.inet_aton(host)
        except OSError:
            _diagnose_dns(host)

    server = _server_part(host, port)
    cs = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={db};UID={user};PWD={pwd};"
        f"Encrypt=Yes;TrustServerCertificate=Yes;Connection Timeout=15;"
    )
    logging.info(f"Conectando a SQL con driver '{driver}' -> SERVER={server}, DB={db}")
    cnx = pyodbc.connect(cs, autocommit=False)
    cnx.autocommit = False
    return cnx

# -------------- OAuth token --------------
def _token_try(url, scope, auth_mode, cid, sec):
    payload = {"grant_type": "client_credentials"}
    if scope:
        payload["scope"] = scope
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    auth = (cid, sec) if auth_mode == "basic" else None
    body = payload if auth_mode == "basic" else {**payload, "client_id": cid, "client_secret": sec}
    return requests.post(url, data=body, headers=headers, auth=auth, timeout=HTTP_TIMEOUT)

def get_token():
    global _cached_token, _token_exp
    cid = (CLIENT_ID or "").strip()
    sec = (CLIENT_SECRET or "").strip()
    if not cid or not sec:
        raise RuntimeError("Faltan SUNAT_CLIENT_ID o SUNAT_CLIENT_SECRET en el .env")
    with _token_lock:
        if _cached_token and (_token_exp - datetime.datetime.now(datetime.timezone.utc)).total_seconds() > 60:
            return _cached_token, _token_exp

        endpoints = [
            f"https://api-seguridad.sunat.gob.pe/v1/clientessol/{cid}/oauth2/token/",
            f"https://api-seguridad.sunat.gob.pe/v1/clientesextranet/{cid}/oauth2/token/",
        ]
        scopes = [
            "https://api.sunat.gob.pe/v1/contribuyente/contribuyentes",
            "https://api.sunat.gob.pe/v1/contribuyente/*",
            None,
        ]
        auth_modes = ["basic", "body"]

        last_err = None
        for ep in endpoints:
            for auth_mode in auth_modes:
                for sc in scopes:
                    try:
                        r = _token_try(ep, sc, auth_mode, cid, sec)
                        if r.status_code == 200:
                            js = r.json()
                            _cached_token = js["access_token"]
                            _token_exp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                                seconds=int(js.get("expires_in", 0)) or 0
                            )
                            logging.info(f"Token SUNAT renovado (endpoint={ep.split('/v1/')[1].split('/')[0]}, auth={auth_mode}, scope={sc or 'sin scope'})")
                            return _cached_token, _token_exp
                        last_err = f"[{ep} | auth={auth_mode} | scope={sc}] -> HTTP {r.status_code} - {r.text[:800]}"
                        logging.warning(f"Token fail: {last_err}")
                    except requests.RequestException as e:
                        last_err = f"[{ep} | auth={auth_mode} | scope={sc}] -> excepción: {repr(e)}"
                        logging.warning(f"Token exception: {last_err}")
        raise RuntimeError(f"No se pudo obtener token SUNAT. Último error: {last_err}")

# -------------- Cola ---------------------
def fetch_batch(cnx, n):
    c = cnx.cursor()
    c.execute(f"""
UPDATE {T_QUEUE}
SET Status='processing', Attempts=Attempts+1
OUTPUT inserted.IdQueue, inserted.IdFactura, inserted.RUC_Emisor, inserted.RUC_Receptor,
       inserted.TipoDocumento, inserted.Serie, inserted.Numero, inserted.FechaEmision, inserted.ImporteTotal
WHERE IdQueue IN (
  SELECT TOP (?) IdQueue FROM {T_QUEUE} WITH (READPAST, ROWLOCK)
  WHERE Status='queued' ORDER BY EnqueuedAt
)""", (n,))
    rows = c.fetchall()
    c.close()
    return rows

# -------------- Body Postman -------------
def to_body_postman(row):
    _, _, ruc_em, _, tip, ser, num, femi, tot = row
    fecha_str = None
    if femi is not None:
        if isinstance(femi, (datetime.datetime, datetime.date)):
            fecha_str = femi.strftime("%d/%m/%Y")
        else:
            fecha_str = datetime.datetime.strptime(str(femi), "%Y-%m-%d").strftime("%d/%m/%Y")
    monto = "0.00" if tot is None else str(Decimal(tot).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP))
    return {
        "numRuc": ruc_em,
        "codComp": tip,
        "numeroSerie": ser,
        "numero": num,
        "fechaEmision": fecha_str,
        "monto": monto
    }

# -------------- Estado SUNAT -------------
def _as_str(v):
    if v is None:
        return None
    try:
        return str(int(v))
    except Exception:
        s = str(v).strip()
        return s if s else None

def map_estado(js):
    """
    Mapea 'data.estadoCp':
      0: NO EXISTE | 1: ACEPTADO | 2: ANULADO | 3: AUTORIZADO | 4: NO AUTORIZADO
    """
    data = js.get("data") if isinstance(js, dict) else None
    estado_cp = _as_str(data.get("estadoCp")) if isinstance(data, dict) else None
    catalogo = {
        "0": ("NO EXISTE",     "NO EXISTE (0)"),
        "1": ("ACEPTADO",      "ACEPTADO (1)"),
        "2": ("ANULADO",       "ANULADO (2)"),
        "3": ("AUTORIZADO",    "AUTORIZADO (3)"),
        "4": ("NO AUTORIZADO", "NO AUTORIZADO (4)"),
    }
    if estado_cp in catalogo:
        nom, desc = catalogo[estado_cp]
        return nom, desc, estado_cp
    if estado_cp is None:
        return None, None, None
    return f"CODE_{estado_cp}", f"NO_MAPEADO ({estado_cp})", estado_cp

# -------------- HTTP ---------------------
def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {"raw": (resp.text or "")[:1000]}

def call_sunat(headers, body):
    last_exc = None
    for i in range(RETRY_MAX):
        try:
            r = requests.post(VALIDA_URL, headers=headers, json=body, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return True, r.json()
            return False, {"http": r.status_code, **safe_json(r)}
        except requests.RequestException as e:
            last_exc = e
            time.sleep(2 * (i + 1))
    return False, {"error": str(last_exc) if last_exc else "unknown"}

# -------------- Historial ----------------
def insert_hist(cnx, row, token_expira_utc, js):
    """
    Guarda histórico; Codigo_Respuesta = estadoCp (0–4) cuando existe.
    """
    _, idf, ruc_em, ruc_rec, tip, ser, num, femi, tot = row
    estado_txt, estado_desc, estado_cp = map_estado(js)
    codigo_respuesta = estado_cp
    mensaje = js.get("message") or js.get("mensaje") or js.get("observacion")

    c = cnx.cursor()
    c.execute(f"""
INSERT INTO {T_HIST}
 (IdFactura,RUC_Emisor,RUC_Receptor,TipoDocumento,Serie,Numero,FechaEmision,ImporteTotal,
  Estado_SUNAT,Codigo_Respuesta,Mensaje,Token_Expira_UTC,Raw_JSON)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
              (idf, ruc_em, ruc_rec, tip, ser, num, femi, tot,
               estado_txt, codigo_respuesta, mensaje, token_expira_utc, json.dumps(js, ensure_ascii=False)))
    c.close()
    return estado_txt, estado_desc, codigo_respuesta, mensaje

# -------------- Snapshot -----------------
def upsert_snapshot(cnx, row, estado_txt, estado_desc, codigo_respuesta, mensaje):
    _, idf, ruc_em, ruc_rec, tip, ser, num, femi, tot = row
    ahora = datetime.datetime.now(datetime.timezone.utc)
    cur = cnx.cursor()
    cur.execute(f"SELECT Estado_Actual, Estado_Descripcion FROM {T_SNAPSHOT} WHERE IdFactura=?", (idf,))
    prev = cur.fetchone()

    if prev is None:
        cur.execute(f"""
INSERT INTO {T_SNAPSHOT}
 (IdFactura,RUC_Emisor,RUC_Receptor,TipoDocumento,Serie,Numero,ImporteTotal,
  Estado_Actual,Estado_Descripcion,Codigo_Respuesta,Mensaje,
  Fecha_Primera_Consulta,Fecha_Ultima_Consulta,Fecha_Ultimo_Cambio,Cambio_Estado)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (idf, ruc_em, ruc_rec, tip, ser, num, tot,
             estado_txt, estado_desc, codigo_respuesta, mensaje,
             ahora, ahora, ahora, 1 if estado_txt else 0))
    else:
        prev_estado, prev_desc = prev
        cambio = (prev_estado or "") != (estado_txt or "") or (prev_desc or "") != (estado_desc or "")
        if cambio:
            cur.execute(f"""
UPDATE {T_SNAPSHOT}
SET Estado_Actual=?,Estado_Descripcion=?,Codigo_Respuesta=?,Mensaje=?,
    Fecha_Ultima_Consulta=?,Fecha_Ultimo_Cambio=?,Cambio_Estado=1
WHERE IdFactura=?""",
                (estado_txt, estado_desc, codigo_respuesta, mensaje, ahora, ahora, idf))
        else:
            cur.execute(f"""
UPDATE {T_SNAPSHOT}
SET Codigo_Respuesta=?,Mensaje=?,Fecha_Ultima_Consulta=?,Cambio_Estado=0
WHERE IdFactura=?""",
                (codigo_respuesta, mensaje, ahora, idf))
    cur.close()

# -------------- UPDATE final desde Python --------------
def update_final_from_snapshot(cnx):
    """
    Actualiza SOLO columnas SUNAT_* en DATA.FACTURA_COMPRA_BACKUS_CABECERA
    desde INH.SUNAT_ESTADO_ACTUAL usando OUTPUT a #upd para tener un result set
    confiable (evita 'No results. Previous SQL was not a query').
    """
    cur = cnx.cursor()

    # 1) Diagnóstico previo (opcional pero útil)
    cur.execute(f"""
;WITH SRC AS (
    SELECT
        s.IdFactura,
        s.Estado_Actual,
        s.Estado_Descripcion,
        s.Codigo_Respuesta,
        s.Mensaje,
        s.Fecha_Primera_Consulta,
        s.Fecha_Ultima_Consulta,
        s.Fecha_Ultimo_Cambio,
        s.Cambio_Estado
    FROM {T_SNAPSHOT} s WITH (NOLOCK)
)
SELECT COUNT(*)
FROM {T_FINAL} d
JOIN SRC s ON s.IdFactura = d.IdFactura
WHERE
    ISNULL(d.Estado_SUNAT_ULT,'')         <> ISNULL(s.Estado_Actual,'')
 OR ISNULL(d.Estado_SUNAT_Descripcion,'') <> ISNULL(s.Estado_Descripcion,'')
 OR ISNULL(CAST(d.SUNAT_Cambio_Estado AS int),-1) <> ISNULL(s.Cambio_Estado,0)
 OR ISNULL(d.SUNAT_Codigo_Respuesta,'')   <> ISNULL(s.Codigo_Respuesta,'')
 OR ISNULL(d.SUNAT_Mensaje,'')            <> ISNULL(s.Mensaje,'')
 OR (d.SUNAT_Fecha_Primera IS NULL AND s.Fecha_Primera_Consulta IS NOT NULL)
 OR (d.SUNAT_Fecha_Primera IS NOT NULL AND s.Fecha_Primera_Consulta IS NULL)
 OR (d.SUNAT_Fecha_Primera IS NOT NULL AND s.Fecha_Primera_Consulta IS NOT NULL AND d.SUNAT_Fecha_Primera <> s.Fecha_Primera_Consulta)
 OR (d.SUNAT_Fecha_Ultima  IS NULL AND s.Fecha_Ultima_Consulta  IS NOT NULL)
 OR (d.SUNAT_Fecha_Ultima  IS NOT NULL AND s.Fecha_Ultima_Consulta  IS NULL)
 OR (d.SUNAT_Fecha_Ultima  IS NOT NULL AND s.Fecha_Ultima_Consulta  IS NOT NULL AND d.SUNAT_Fecha_Ultima  <> s.Fecha_Ultima_Consulta)
 OR (s.Cambio_Estado = 1 AND (d.SUNAT_Fecha_Cambio IS NULL OR d.SUNAT_Fecha_Cambio <> s.Fecha_Ultimo_Cambio));
""")
    to_fix = cur.fetchone()[0]
    logging.info(f"[FINAL] Filas con diferencias a actualizar: {to_fix}")

    # 2) UPDATE con OUTPUT a temp table y SELECT COUNT(*) (un solo result set)
    cur.execute(f"""
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

IF OBJECT_ID('tempdb..#upd') IS NOT NULL DROP TABLE #upd;
CREATE TABLE #upd (IdFactura INT PRIMARY KEY);

;WITH SRC AS (
    SELECT
        s.IdFactura,
        s.Estado_Actual,
        s.Estado_Descripcion,
        s.Codigo_Respuesta,
        s.Mensaje,
        s.Fecha_Primera_Consulta,
        s.Fecha_Ultima_Consulta,
        s.Fecha_Ultimo_Cambio,
        s.Cambio_Estado
    FROM {T_SNAPSHOT} s WITH (NOLOCK)
)
UPDATE d
   SET d.Estado_SUNAT_ULT         = s.Estado_Actual,
       d.Estado_SUNAT_Descripcion = s.Estado_Descripcion,
       d.SUNAT_Codigo_Respuesta   = s.Codigo_Respuesta,
       d.SUNAT_Mensaje            = s.Mensaje,
       d.SUNAT_Cambio_Estado      = CASE WHEN s.Cambio_Estado = 1 THEN 1 ELSE 0 END,
       d.SUNAT_Fecha_Primera      = COALESCE(d.SUNAT_Fecha_Primera, s.Fecha_Primera_Consulta),
       d.SUNAT_Fecha_Ultima       = s.Fecha_Ultima_Consulta,
       d.SUNAT_Fecha_Cambio       = CASE WHEN s.Cambio_Estado = 1
                                         THEN s.Fecha_Ultimo_Cambio
                                         ELSE d.SUNAT_Fecha_Cambio
                                    END
  OUTPUT inserted.IdFactura INTO #upd(IdFactura)
  FROM {T_FINAL} AS d WITH (ROWLOCK)
  JOIN SRC s ON s.IdFactura = d.IdFactura;

SELECT COUNT(*) AS Affected FROM #upd;
""")
    affected = cur.fetchone()[0] if cur.description else 0

    cnx.commit()
    cur.close()
    logging.info(f"[FINAL] Columnas SUNAT actualizadas en {affected} filas de {T_FINAL}")

# -------------- Cola status --------------
def mark_done(cnx, idq, _unused=None):
    cnx.cursor().execute(f"UPDATE {T_QUEUE} SET Status='done', LastError=NULL WHERE IdQueue=?", (idq,))

def mark_error(cnx, idq, err):
    if isinstance(err, dict):
        err = json.dumps(err, ensure_ascii=False)[:3900]
    cnx.cursor().execute(f"UPDATE {T_QUEUE} SET Status='error', LastError=? WHERE IdQueue=?", (str(err)[:3900], idq))

# -------------- Lote ---------------------
def process_batch(cnx):
    token, token_exp = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    rows = fetch_batch(cnx, WORKER_BATCH)
    if not rows:
        return 0, 0, 0

    ok_cnt = err_cnt = 0
    with ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
        futures = {ex.submit(call_sunat, headers, to_body_postman(r)): r for r in rows}
        for fut in as_completed(futures):
            row = futures[fut]
            idq = row[0]
            try:
                ok, js = fut.result()
                estado_txt, estado_desc, codigo_respuesta, mensaje = insert_hist(cnx, row, token_exp, js)
                upsert_snapshot(cnx, row, estado_txt, estado_desc, codigo_respuesta, mensaje)
                (mark_done if ok else mark_error)(cnx, idq, js)
                cnx.commit()
                ok_cnt += 1 if ok else 0
                err_cnt += 0 if ok else 1
            except Exception as e:
                cnx.rollback()
                mark_error(cnx, idq, e)
                cnx.commit()
                err_cnt += 1

    # Actualiza tabla final desde Python (idempotente)
    try:
        update_final_from_snapshot(cnx)
    except Exception:
        logging.exception("Fallo actualizando tabla final desde snapshot")

    return len(rows), ok_cnt, err_cnt

# -------------- Main ---------------------
def main():
    cnx = sql_cnx()
    try:
        # chequeo rápido
        cur = cnx.cursor(); cur.execute("SELECT 1"); logging.info(f"SQL OK -> {cur.fetchone()[0]}"); cur.close()

        while True:
            total, okc, errc = process_batch(cnx)
            if total == 0:
                time.sleep(5)
            else:
                logging.info(f"Lote: total={total} ok={okc} err={errc}")
    finally:
        cnx.close()

if __name__ == "__main__":
    try:
        with sql_cnx() as _c:
            cur = _c.cursor(); cur.execute("SELECT 1"); logging.info(f"SQL OK -> {cur.fetchone()[0]}"); cur.close()
    except Exception:
        logging.exception("Fallo de conexión SQL (revisa SQL_SERVER/PORT/DB/USER/PASS, firewall y driver ODBC)")
        raise
    main()
