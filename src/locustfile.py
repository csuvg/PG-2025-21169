import os, json, random, string
from datetime import date, timedelta
from locust import HttpUser, task, between, events
import gevent.lock

HOST = os.getenv("HOST", "http://127.0.0.1:8081")
LOGIN_PATH = "/api/auth/login/"
TOKEN_PREFIX = os.getenv("TOKEN_PREFIX", "Bearer")

CATEGORY_BASE = "/api/categorias/"
FORM_BASE     = "/api/formularios/"
PAGE_BASE     = "/api/paginas/"
FIELD_BASE    = "/api/campos/"
PAGE_ADD_FIELD_ACTION = "campos"

# ====== credenciales ======
CRED_LIST = []
@events.test_start.add_listener
def _parse_creds(environment, **kw):
    raw = (os.getenv("API_USERS", "") or "").strip()
    pairs = [tuple(x.split(":", 1)) for x in raw.split(",") if ":" in x]
    environment.parsed_creds = pairs
    global CRED_LIST
    CRED_LIST = pairs

# ====== token pool (compartido) ======
_token_pool = {}               # {"user": "TOKEN ..."}
_token_lock = gevent.lock.Semaphore()

def _ensure_slash(p): return p if p.endswith("/") else p + "/"
def rstr(n=6): return "".join(random.choices(string.ascii_lowercase+string.digits,k=n))

def get_or_login(client, user, pwd):
    """Devuelve un token para 'user' reusando el pool; si no hay, loguea y lo guarda."""
    with _token_lock:
        tok = _token_pool.get(user)
        if tok:
            return tok
    # no hay token: login una sola vez
    payload = {"nombre_usuario": user, "password": pwd}
    with client.post(_ensure_slash(LOGIN_PATH), json=payload, name="AUTH login", catch_response=True) as resp:
        if resp.status_code != 200:
            resp.failure(f"{resp.status_code} {resp.text}"); return None
        data = resp.json() if resp.headers.get("content-type","").startswith("application/json") else {}
        token = data.get("access_token") or data.get("access") or data.get("token") or data.get("key")
        if not token:
            resp.failure(f"Login sin token. JSON={data}"); return None
        tok = f"{TOKEN_PREFIX} {token}"
        resp.success()
    with _token_lock:
        _token_pool[user] = tok
    return tok

def pick_cred(idx):
    if not CRED_LIST:
        return (os.getenv("API_USER",""), os.getenv("API_PASS",""))
    return CRED_LIST[idx % len(CRED_LIST)]

class WebUser(HttpUser):
    host = HOST
    wait_time = between(0.2, 0.8)

    def on_start(self):
        self.client.headers.update({"Accept": "application/json","Content-Type":"application/json"})
        self.auth_ok = False
        # asigna cred por índice de VU (id(self) no es consecutivo; usa environment.runner.user_count_index si la tienes,
        # aquí usamos un contador global simple)
        if not hasattr(WebUser, "_vu_counter"):
            WebUser._vu_counter = 0
            WebUser._vu_lock = gevent.lock.Semaphore()
        with WebUser._vu_lock:
            self.vu_idx = WebUser._vu_counter
            WebUser._vu_counter += 1

        self.user, self.pwd = pick_cred(self.vu_idx)

        # toma token del pool o hace login una vez
        tok = get_or_login(self.client, self.user, self.pwd)
        if not tok: return
        self.client.headers["Authorization"] = tok

        # ✅ habilita las tareas
        self.auth_ok = True



    # reintenta una vez ante 401 → relogin + actualiza token pool
    def _request(self, method, url, **kw):
        r = self.client.request(method, url, **kw)
        if r.status_code == 401:
            tok = get_or_login(self.client, self.user, self.pwd)
            if tok:
                self.client.headers["Authorization"] = tok
                r = self.client.request(method, url, **kw)
        return r

    # ===== helpers CRUD =====
    def _crear_categoria(self):
        b = {"nombre": f"cat_{rstr()}", "descripcion": "locust categoria"}
        r = self._request("POST", _ensure_slash(CATEGORY_BASE), json=b, name="POST /categorias/")
        if r.status_code in (200,201):
            d=r.json(); return d.get("id") or d.get("id_categoria") or d.get("uuid")
        return None

    def _crear_formulario(self, cid=None):
        hoy = date.today()
        b = {"nombre": f"Form {rstr()}", "descripcion":"locust","permitir_fotos":True,"permitir_gps":True,
             "disponible_desde_fecha": hoy.isoformat(),
             "disponible_hasta_fecha": (hoy+timedelta(days=30)).isoformat(),
             "estado":"Activo","forma_envio":"En Linea","es_publico":False,"auto_envio":False}
        if cid: b["id_categoria"]=cid
        r = self._request("POST", _ensure_slash(FORM_BASE), json=b, name="POST /formularios/")
        if r.status_code in (200,201):
            d=r.json(); return d.get("id") or d.get("id_formulario") or d.get("uuid")
        return None

    def _agregar_pagina(self, fid):
        return self._request("POST", f"{_ensure_slash(FORM_BASE)}{fid}/agregar-pagina/",
                             json={"nombre": f"Pag {rstr()}","descripcion":"locust page"},
                             name="POST /formularios/{id}/agregar-pagina/")

    def _agregar_campo_a_pagina(self, pid):
        body = {"nombre_campo": f"campo_{rstr()}","etiqueta":"Etiqueta prueba","clase":"number",
                "ayuda":"Ayuda...","requerido":True,"config":{"min":31,"max":87,"step":None,"unit":"$"}}
        return self._request("POST", f"{_ensure_slash(PAGE_BASE)}{pid}/{PAGE_ADD_FIELD_ACTION}/",
                             json=body, name="POST /paginas/{id}/campos/")

    # ===== escenarios =====
    @task(3)
    def flujo_escritura(self):
        if not self.auth_ok: return
        cid = self._crear_categoria()
        fid = self._crear_formulario(cid)
        if not fid: return
        self._request("GET", f"{_ensure_slash(FORM_BASE)}{fid}/", name="GET /formularios/{id}/")
        rp = self._agregar_pagina(fid)
        pid = rp.json().get("id_pagina") if rp.status_code in (200,201) else None
        field_id = None
        if pid:
            rc = self._agregar_campo_a_pagina(pid)
            if rc.status_code in (200,201):
                field_id = rc.json().get("id_campo")
        self._request("PATCH", f"{_ensure_slash(FORM_BASE)}{fid}/",
                      json={"descripcion":"edit locust"}, name="PATCH /formularios/{id}/")
        if pid:
            self._request("PATCH", f"{_ensure_slash(PAGE_BASE)}{pid}/",
                          json={"descripcion":"edit page locust"}, name="PATCH /paginas/{id}/")
        if field_id:
            self._request("PATCH", f"{_ensure_slash(FIELD_BASE)}{field_id}/",
                          json={"etiqueta":"Etiqueta edit"}, name="PATCH /campos/{id}/")
            self._request("DELETE", f"{_ensure_slash(FIELD_BASE)}{field_id}/", name="DELETE /campos/{id}/")
        if pid:
            self._request("DELETE", f"{_ensure_slash(PAGE_BASE)}{pid}/", name="DELETE /paginas/{id}/")
        self._request("DELETE", f"{_ensure_slash(FORM_BASE)}{fid}/", name="DELETE /formularios/{id}/")

    @task(1)
    def solo_listas(self):
        if not self.auth_ok: return
        self._request("GET", _ensure_slash(CATEGORY_BASE), name="GET /categorias/")
        self._request("GET", _ensure_slash(FORM_BASE), name="GET /formularios/")
        self._request("GET", _ensure_slash(PAGE_BASE), name="GET /paginas/")
