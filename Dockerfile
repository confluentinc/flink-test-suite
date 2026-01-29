FROM maven:3.9-eclipse-temurin-17

# Install dependencies and add deadsnakes PPA for Python 3.11
RUN apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update && apt-get install -y \
    python3.11 \
    python3.11-venv \
    python3.11-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment with Python 3.11
RUN python3.11 -m venv /opt/venv

# Activate venv by adding to PATH
ENV PATH="/opt/venv/bin:$PATH"

# Install PyFlink in the virtual environment
RUN pip install --upgrade pip && \
    pip install apache-flink==2.1.1

# Set working directory
WORKDIR /app

# Copy pom.xml first for dependency caching
COPY pom.xml .

# Download dependencies (cached layer)
RUN mvn dependency:go-offline -B || true

# Copy source code
COPY src/ src/

# Build the project (compile tests)
RUN mvn test-compile -DskipTests

# Create directories for mounted volumes
RUN mkdir -p /app/test-cases /app/udf-jars /app/python-udf

# Default command (can be overridden in docker-compose)
CMD ["mvn", "test", "-Dtest=FlinkKafkaFileBasedTest"]