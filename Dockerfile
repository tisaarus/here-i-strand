FROM public.ecr.aws/docker/library/python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_COMPILE_BYTECODE=0 \
    AWS_DEFAULT_REGION=eu-central-1

# Instalar dependencias del sistema básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    ca-certificates \
    fonts-liberation \
    fonts-noto \
    fonts-noto-cjk \
    fonts-noto-color-emoji \
    locales \
    gnupg \
    libxkbcommon0 \
    libxcomposite1 \
    libxrandr2 \
    libxdamage1 \
    libxfixes3 \
    libgtk-3-0 \
    libgbm1 \
    libasound2 \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && npm cache clean --force \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/* /root/.npm /tmp/* /var/tmp/*
    
# Instalar uv
RUN pip install --no-cache-dir uv

# Configurar AWS
RUN mkdir -p ~/.aws \
    && echo '[default]\nregion = eu-central-1' > ~/.aws/config

COPY requirements.txt .

# Instalar dependencias Python
RUN uv pip install -r requirements.txt --system

# Copiar el resto del código
COPY . .

# Ejecutar el script
CMD ["opentelemetry-instrument","python", "main.py"]