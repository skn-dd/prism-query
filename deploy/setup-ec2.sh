#!/bin/bash
# Prism SF100 Benchmark — EC2 Setup Script
# Target: r7g.4xlarge (16 vCPU ARM, 128 GB RAM) or similar
#
# Usage:
#   1. Launch EC2 instance (Amazon Linux 2023 ARM or Ubuntu 24.04 ARM)
#   2. scp this repo to the instance
#   3. Run: bash deploy/setup-ec2.sh
#   4. Run: bash deploy/run-sf100.sh

set -euo pipefail

echo "=== Prism SF100 Benchmark Setup ==="

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS="unknown"
fi

echo "OS: $OS"

# Install dependencies
if [ "$OS" = "amzn" ] || [ "$OS" = "rhel" ]; then
    sudo yum install -y gcc gcc-c++ make cmake protobuf-compiler git java-23-amazon-corretto-headless 2>/dev/null || \
    sudo yum install -y gcc gcc-c++ make cmake protobuf-compiler git java-21-amazon-corretto-headless
elif [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
    sudo apt-get update
    sudo apt-get install -y build-essential cmake protobuf-compiler git openjdk-21-jdk-headless
fi

# Install Rust
if ! command -v cargo &>/dev/null; then
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
fi

echo "Rust: $(rustc --version)"
echo "Java: $(java -version 2>&1 | head -1)"

# Build Rust workers
echo ""
echo "=== Building Rust Workers ==="
cd native
cargo build --release -p prism-bench
cd ..

echo ""
echo "=== Building Java Trino Plugin ==="
cd trino/prism-bridge
./gradlew shadowJar 2>/dev/null || ./mvnw package -DskipTests 2>/dev/null || echo "Build Java manually if needed"
cd ../..

# Download Trino if not present
TRINO_VERSION=468
if [ ! -d "trino-server-${TRINO_VERSION}" ]; then
    echo ""
    echo "=== Downloading Trino ${TRINO_VERSION} ==="
    curl -O "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"
    tar xzf "trino-server-${TRINO_VERSION}.tar.gz"
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Configure Trino (see trino-bench/coordinator/ for example config)"
echo "  2. Run: bash deploy/run-sf100.sh"
