# Python 3.13 sobre Debian 12 (bookworm)
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# ----- SO: paquetes necesarios para ODBC y build de pyodbc -----
# - unixodbc / unixodbc-dev -> libodbc.so.2 y headers
# - curl, gnupg -> para agregar repo de Microsoft
# - libgssapi-krb5-2 -> dependencia de msodbcsql
# - tzdata y ca-certificates -> SSL/horario
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg apt-transport-https ca-certificates tzdata \
    unixodbc unixodbc-dev \
    libgssapi-krb5-2 \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ----- Agregar repositorio de Microsoft y driver msodbcsql18 -----
# (para Debian 12 / bookworm)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/msprod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/msprod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/microsoft.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

# (Opcional) ODBC tools para diagnosticar: isql, odbcinst
# RUN apt-get update && apt-get install -y --no-install-recommends odbcinst unixodbc-bin && rm -rf /var/lib/apt/lists/*

# Copiamos requirements primero para cachear la capa
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código
COPY . .

# Salud del contenedor (opcional: intenta importar pyodbc)
HEALTHCHECK --interval=30s --timeout=10s --retries=3 CMD python -c "import pyodbc" || exit 1

# Arranque
CMD ["python", "app.py"]
