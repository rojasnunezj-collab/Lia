# ====================================================================
# --- IMAGEN BASE ---
# ====================================================================
FROM python:3.11-slim

# ====================================================================
# --- VARIABLES DE ENTORNO DEL SISTEMA ---
# ====================================================================
# Evita que Python genere archivos .pyc y asegura logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8

# ====================================================================
# --- DEPENDENCIAS DEL SISTEMA ---
# ====================================================================
# Pillow requiere libjpeg y zlib en Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    libjpeg-dev \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ====================================================================
# --- DIRECTORIO DE TRABAJO ---
# ====================================================================
WORKDIR /app

# ====================================================================
# --- DEPENDENCIAS PYTHON ---
# ====================================================================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ====================================================================
# --- CÓDIGO FUENTE ---
# ====================================================================
# Las credenciales (.env, credenciales_lia.json, token.json)
# se montan como volúmenes en el docker run, NO se copian aquí.
COPY . .

# ====================================================================
# --- PUNTO DE ENTRADA ---
# ====================================================================
CMD ["python", "-u", "main.py"]
