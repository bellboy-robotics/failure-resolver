FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/home/resolver \
    PORT=8000 \
    RESOLVER_MODE=agent

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 resolver \
    && useradd \
        --uid 10001 \
        --gid resolver \
        --create-home \
        --home-dir /home/resolver \
        --shell /usr/sbin/nologin \
        resolver \
    && install --directory \
        --owner resolver \
        --group resolver \
        /var/lib/failure-resolver

COPY requirements-observer.txt .
RUN pip install --no-cache-dir -r requirements-observer.txt

COPY --chown=resolver:resolver \
    agent.py \
    memory_store.py \
    observer.py \
    resolver.py \
    git-askpass.sh \
    ./

RUN chmod 0555 /app/git-askpass.sh

USER 10001:10001

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.getenv('PORT', '8000') + '/health', timeout=2).read()"]

CMD ["python", "-m", "resolver"]
