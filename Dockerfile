ARG WORKDIR=/app
FROM python:3.14-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
# uv: copy deps instead of hardlinking (safe across mount boundaries), use the
# image's system Python rather than fetching a managed one, and put the project
# venv at a fixed path so the interpreter location is predictable.
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# Pin a digest or version tag in real use; :latest is not reproducible.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN apt-get update && apt-get install -y \
    curl \
    git \
    vim \
    build-essential \
    sudo \
    && rm -rf /var/lib/apt/lists/*
ARG WORKDIR
WORKDIR ${WORKDIR}
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo "$USERNAME ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME
FROM base AS dependencies
# Cache third-party deps on the lock alone; the root package is installed later
# so source edits don't bust this layer.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev
FROM dependencies AS development
COPY . .
RUN uv sync --frozen
RUN git config --system --add safe.directory /workspaces/c3d-mcp \
    && chown -R vscode:vscode ${WORKDIR} /opt/venv
USER vscode
FROM dependencies AS production
ENV UV_COMPILE_BYTECODE=1
COPY c3d_mcp/ ./c3d_mcp/
COPY README.md ./
RUN uv sync --frozen --no-dev --no-editable
