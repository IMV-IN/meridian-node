# Non-root agent image. Observe-only by default; managed-host mode additionally
# requires the root helper (run on the host, not in this container) and a
# root-owned policy file. See DESIGN.md 6.5 and 14.1.
# Python 3.14 is the project's runtime everywhere (image, CI, local dev).
FROM python:3.14-slim-trixie

RUN useradd --system --uid 10001 --create-home meridian-node
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[http]"

USER meridian-node
ENTRYPOINT ["meridian-node", "--config", "/etc/meridian-node/config.yaml"]
CMD ["run"]
