FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PORT=8000 \
    RESOLVER_MODE=observe

WORKDIR /app

RUN groupadd --gid 10001 resolver \
    && useradd --uid 10001 --gid resolver --no-create-home --shell /usr/sbin/nologin resolver

COPY requirements-observer.txt .
RUN pip install --no-cache-dir -r requirements-observer.txt

COPY --chown=resolver:resolver observer.py .

USER 10001:10001

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('PORT', '8000') + '/health', timeout=2).read()"]

CMD ["python", "-m", "observer"]
