# Imagen única parametrizada para todos los microservicios (ARG SERVICE).
FROM python:3.13-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && adduser --disabled-password --gecos "" --uid 10001 appuser
COPY common ./common
COPY services ./services
COPY frontend ./frontend
ARG SERVICE
ENV SERVICE=${SERVICE} FRONTEND_DIR=/app/frontend
# Ejecución como usuario sin privilegios (principio de menor privilegio).
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://localhost:8000/health', timeout=2)" || exit 1
CMD ["sh", "-c", "uvicorn services.${SERVICE}.main:app --host 0.0.0.0 --port 8000 --workers ${WORKERS:-2} --no-access-log"]
