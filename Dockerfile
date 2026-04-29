FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    librdkafka-dev \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*


# Copy dependency files
COPY requirements.txt ./
COPY main.py .
COPY src/ ./src/

# Install dependencies using uv
RUN uv pip install --system -r requirements.txt
CMD ["python", "main.py"]
