# Santa Ana AgroForms Backend (Django REST)

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Django](https://img.shields.io/badge/Django-5.x-0C4B33)
![DRF](https://img.shields.io/badge/DRF-3.16-red)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED)

> Backend para la plataforma web **Santa Ana AgroForms**: creación, edición y gestión de formularios consumidos por una app móvil (con soporte offline). Incluye otras funciones como exportación de respuestas, creación de usuarios, accesos, asignaciones de formularios, uso de datasets externos (Excel), autenticación OAuth2 y documentación OpenAPI.

---

## ✨ Características clave

* **Gestión de Formularios**:
  * Acciones: crear formularios, páginas, campos, duplicación, edición y eliminación de formularios.
* **Campos avanzados**:
  * **Campos de Tipo Grupo** que agrupa campos dentro de ellos.
  * **Fuentes de Datos** para autocompletar campos desde data de un Excel.
* **Asignaciones**: asigna formularios a usuarios (multiselección) y controla su disponibilidad.
* **Exportaciones**: descarga de respuestas en **Excel**.
* **Autenticación**: OAuth2 (django-oauth-toolkit).
* **Documentación**: Swagger UI / Redoc servidos desde el backend (`drf-spectacular`).
---

## 🧱 Stack

* **Python 3.11**, **Django 5.x**, **Django REST Framework 3.16**
* **drf-spectacular** para OpenAPI 3
* **django-oauth-toolkit** para OAuth2
* **Azure Blob Storage** (SDK oficial) para datasets Excel
* **PostgreSQL** como base de datos

---

## 📚 Endpoints principales

* `POST /api/formularios/` → creación de un formulario.
* `POST /api/formularios/{id}/duplicar/` → duplica un formulario específico completo.
* `POST /api/formularios/{id}/agregar-pagina/` → crea una página en un formulario en específico.
* `POST /api/paginas/{id}/campos/` → agrega campo en una página en específico.
* `GET /api/asignaciones/` y `POST /api/asignaciones/crear-asignacion/` → asignaciones de ciertos formularios a los usuarios registrados.
* `POST /api/fuentes-datos` → permite subir archivos de Excel para su uso posterior en campos de autocompletado.
* `POST /api/auth/login` → Ruta para hacer login y obtener acceso a las rutas
* **Docs**: `/api/schema/doc/`.

---

## 💻 Requisitos previos

- Python 3.11+
- Docker Desktop
- Git 2.30+

---

## 🚀 Quickstart

## 🔧 Configuración (.env)

Variables necesarias para correr la API, reemplazar los valores a la derecha por los reales:

```dotenv
DATABASE_HOST=HOST
DATABASE_USER=USER
DATABASE_PASSWORD=PASSWORD
DATABASE_NAME=DB

AZURE_STORAGE_CONNECTION_STRING=STRING
AZURE_CONTAINER=CONTAINER
AZURE_ACCOUNT_NAME=ACCOUNT
AZURE_ACCOUNT_KEY=KEY
```

## 🐳 Imagen desde Docker Hub

Es posible ejecutar la API con la imagen almacenada en Docker Hub de manera local en nuestro equipo. Para ello se necesita tener Docker Desktop instalado y corriendo, ya con ello se puede proceder a realizar el pull de la imagen de las siguientes formas:

> Nota: Tomar en cuenta que ambas formas se deben ejecutar o crear el archivo `.yml` dentro de la carpeta donde se encuentre las credencuales en el archivo .env

### >_ PowerShell

Al tener listo Docker Desktop se deben ejecutar los siguientes comandos desde PowerShell:

```bash
docker pull lindain1333/santa-ana-api
```

```bash
docker run -d `
  --name agroforms-api `
  --env-file "${pwd}\.env" `
  -p 8082:8082 `
  lindain1333/santa-ana-api:latest `
  python manage.py runserver 0.0.0.0:8082
```

### 🐋 Docker Compose

Para utilizar Docker Compose debemos empezar creando nuestro archivo `docker-compose.yml` de la siguiente manera:

```bash
services:
  api:
    image: lindain1333/santa-ana-api:latest
    container_name: agroforms-api2
    env_file: .env
    ports:
      - "${PORT:-8082}:8082"
    command: >
      sh -c "
        python manage.py migrate &&
        python manage.py collectstatic --noinput || true &&
        python manage.py runserver 0.0.0.0:${PORT:-8082}
      "
    healthcheck:
      test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:${PORT:-8082}/api/docs >/dev/null 2>&1 || exit 1"]
      interval: 10s
      timeout: 3s
      retries: 10
```

Luego de ello ejecutamos los siguientes comandos:

```bash
docker compose up -d
docker compose logs -f api         
```

> Si se usa el puerto 8082 en cualquiera de los dos casos anteriores visualizar la API en [http://localhost:8082/api/docs](http://localhost:8082/api/docs), sino reemplazar por el puerto que se coloque

---

## 💻 Desarrollo local (sin Docker)

Si se quiere clonar el proyeto completo desde GitHub se debe abrir una terminal dentro de la carpeta del proyecto y correr los siguientes comandos:

```bash
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt

python manage.py runserver 8081
```

Visita: [http://localhost:8081/api/docs](http://localhost:8081/api/docs)

---

## 🚀 API Desplegada

Visita: [https://santa-ana-api.onrender.com/api/docs](https://santa-ana-api.onrender.com/api/docs). Considerar que se debe usar la ruta de autenticación login con usuario y contraseña, y el access_token devuelto introducirse en la sección de Authorize → BearerAuth para que se pueda tener acceso al uso de rutas.

---

## 🔗 Docker Hub

La imagen oficial se publica en: [https://hub.docker.com/r/lindain1333/santa-ana-api](https://hub.docker.com/r/lindain1333/santa-ana-api)

---

## 📹 Demo

El video demostrativo se encuentra en [demo/demo.mp4](demo/demo.mp4).

---

## 📃 Document

El informe final del proyecto está disponible en [docs/informe_final.pdf](docs/informe_final.pdf)

## 👩🏽‍💻 Autor

* Linda Jiménez [https://github.com/LINDAINES213](https://github.com/LINDAINES213)