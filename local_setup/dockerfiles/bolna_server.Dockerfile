FROM python:3.10.13-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y --no-install-recommends \
    libgomp1 \
    git \
    ffmpeg \
    gcc \
    g++

# Copy ONLY requirements files first
COPY requirements.txt requirements.txt
COPY local_setup/requirements.txt local_setup/requirements.txt

# Install dependencies (this layer will be cached unless requirements files change)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r local_setup/requirements.txt

# Copy the bolna package source code
COPY bolna/ bolna/
COPY pyproject.toml .
COPY README.md .
COPY LICENSE .
COPY local_setup/presets /app/presets


# Install bolna in editable mode
RUN pip install -e .

# Copy the rest of the application code
COPY local_setup/ local_setup/

EXPOSE 5001

CMD ["uvicorn", "local_setup.quickstart_server:app", "--host", "0.0.0.0", "--port", "5001"]