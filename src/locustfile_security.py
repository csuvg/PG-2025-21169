import os
import json
from locust import task, between, HttpUser, tag

PAGE_ID = os.getenv("API_PAGE_ID", "cf4c683f-f51b-4262-9fd2-b5fb3b79b87a")         # UUID de Página
USER_USERNAME = os.getenv("API_USER_USERNAME", "lindain1")
FORM_ID = os.getenv("API_FORM_ID", "e0d8e313-a5ec-4215-b2f3-d77c71dd328d")   # UUID de Formulario

PROTECTED = [
    ("GET",  "/api/formularios/"),
    ("POST", "/api/formularios/"),                 # crear
    ("GET",  "/api/paginas/"),
    ("POST", f"/api/formularios/{FORM_ID}/agregar-pagina/"),# usa un ID válido/placeholder
    ("PATCH", f"/api/formularios/{FORM_ID}/"),
    ("POST", f"/api/formularios/{FORM_ID}/agregar-pagina/"),
    ("POST", f"/api/paginas/{PAGE_ID}/campos/"),
    ("DELETE", f"/api/formularios/{FORM_ID}/"),
    ("POST", "/api/usuarios/"),
    ("PATCH", f"/api/usuarios/{USER_USERNAME}/"),

]

LOGIN_PATH = "/api/auth/login/"

def _headers():
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

class SecurityAuthUser(HttpUser):
    """
    Pruebas NEGATIVAS de autenticación:
    - Credenciales inexistentes  -> debe fallar (400/401/403)
    - Usuario válido sin acceso   -> debe fallar (400/401/403)
    """
    wait_time = between(0.2, 0.8)

    @tag("security", "auth")
    @task
    def login_con_usuario_inexistente(self):
        # Usuario aleatorio que seguro no existe
        bogus_user = "no_existe_" + str(os.getpid())
        payload = {"nombre_usuario": bogus_user, "password": "XyZ12345!"}
        with self.client.post(
            LOGIN_PATH,
            data=json.dumps(payload),
            headers=_headers(),
            name="AUTH NEG: usuario/clave inexistentes",
            catch_response=True
        ) as resp:
            # Aceptamos 400, 401 o 403 como "correcto" para negativo
            if resp.status_code in (400, 401, 403):
                # Y adicionalmente, que NO venga access_token
                try:
                    has_token = bool((resp.json() or {}).get("access_token"))
                except Exception:
                    has_token = False
                if not has_token:
                    resp.success()
                    return
            resp.failure(f"Esperaba 4xx sin token, obtuve {resp.status_code}: {resp.text[:200]}")

    @tag("security", "auth")
    @task
    def login_con_usuario_sin_permiso(self):
        """
        Usa un usuario REAL de tu BD que:
          - exista pero NO tenga acceso_web=True (o esté inactivo)
        Pásalo por variables de entorno:
          API_DENIED_USER, API_DENIED_PASS
        """
        u = os.getenv("API_DENIED_USER")
        p = os.getenv("API_DENIED_PASS")
        if not u or not p:
            # Marcamos failure para que no pase silenciosamente sin probar nada
            raise RuntimeError("Faltan credenciales de prueba: define API_DENIED_USER y API_DENIED_PASS")

        payload = {"nombre_usuario": u, "password": p}
        with self.client.post(
            LOGIN_PATH,
            data=json.dumps(payload),
            headers=_headers(),
            name="AUTH NEG: usuario válido SIN acceso",
            catch_response=True
        ) as resp:
            if resp.status_code in (400, 401, 403):
                try:
                    has_token = bool((resp.json() or {}).get("access_token"))
                except Exception:
                    has_token = False
                if not has_token:
                    resp.success()
                    return
            resp.failure(f"Esperaba 4xx sin token (usuario sin permiso), obtuve {resp.status_code}: {resp.text[:200]}")

class SecurityUser(HttpUser):
    wait_time = between(0.2, 0.6)

    @tag("security")
    @task
    def _assert_unauthorized(self, method, path, name):
        with self.client.request(
            method, path, name=name, catch_response=True
        ) as resp:
            if resp.status_code in (401, 403):
                resp.success()
            else:
                resp.failure(f"Esperado 401/403, obtuvo {resp.status_code}: {resp.text[:200]}")

    @tag("security")
    @task
    def sin_token(self):
        for m, p in PROTECTED:
            self._assert_unauthorized(m, p, f"[NO TOKEN] {m} {p}")

    @tag("security")
    @task
    def token_invalido(self):
        # Token inventado
        headers = {"Authorization": "Bearer xyz.invalid.token"}
        for m, p in PROTECTED:
            with self.client.request(m, p, headers=headers, name=f"[BAD TOKEN] {m} {p}", catch_response=True) as resp:
                if resp.status_code in (401, 403):
                    resp.success()
                else:
                    resp.failure(f"Esperado 401/403, obtuvo {resp.status_code}: {resp.text[:200]}")
