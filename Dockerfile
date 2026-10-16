FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY . .

RUN python -m compileall -q tracelens tests \
    && python -m unittest discover -v \
    && groupadd --system --gid 10001 tracelens \
    && useradd --system --uid 10001 --gid tracelens --home-dir /app tracelens

USER 10001:10001
ENTRYPOINT ["python", "-m", "tracelens"]
CMD ["demo"]
