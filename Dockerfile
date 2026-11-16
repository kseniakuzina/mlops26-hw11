# Базовый образ закреплён по digest (python:3.12-slim, 2026-11-05).
FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY app/requirements.lock /app/requirements.lock
RUN pip install --no-deps -r /app/requirements.lock && useradd -u 10001 -M app
COPY app/*.py /app/
# Артефакты имитатора и их хеши: источник версии для всех реплик — неизменяемый образ.
COPY models/ /models/
RUN cd /models && sha256sum -c SHA256SUMS
USER 10001
EXPOSE 8000
CMD ["python", "/app/service.py"]
