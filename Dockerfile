# Imagen base oficial de Python
FROM python:3.13-slim

# Evitar buffer en logs
ENV PYTHONUNBUFFERED=1

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements primero (aprovecha cache de docker)
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . .

# Comando de inicio
CMD ["python", "app.py"]
