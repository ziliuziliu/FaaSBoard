#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get install -y \
  git \
  g++ \
  zip \
  unzip \
  numactl \
  cmake \
  libgflags-dev \
  libssl-dev \
  libcurl4-openssl-dev \
  libboost-all-dev \
  build-essential \
  zlib1g-dev \
  libssl-dev \
  ninja-build \
  wget \
  unzip
sudo rm -rf /var/lib/apt/lists/*

# install docker
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Install AWS CLI v2
rm -rf /tmp/aws /tmp/awscliv2.zip

case "$(uname -m)" in
  x86_64)
    aws_cli_arch="x86_64"
    ;;
  aarch64|arm64)
    aws_cli_arch="aarch64"
    ;;
  *)
    echo "Unsupported architecture for AWS CLI: $(uname -m)"
    exit 1
    ;;
esac

wget -O /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-${aws_cli_arch}.zip"
unzip -q /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install --update
rm -rf /tmp/aws /tmp/awscliv2.zip

# Clone and install glog
rm -rf /tmp/glog

git clone https://github.com/google/glog.git /tmp/glog
cd /tmp/glog
cmake .
make
sudo make install
rm -rf /tmp/glog

# Install AWS SDK C++
rm -rf /tmp/aws-sdk-cpp

git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git /tmp/aws-sdk-cpp
cd /tmp/aws-sdk-cpp
mkdir build
cd build
cmake .. -DBUILD_ONLY="s3;ecs;lambda" \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build . --config=Debug
sudo cmake --install . --config=Debug
rm -rf /tmp/aws-sdk-cpp

# 8. Install AWS Lambda C++ Runtime
git clone https://github.com/awslabs/aws-lambda-cpp.git /tmp/aws-lambda-cpp && \
cd /tmp/aws-lambda-cpp && \
mkdir build && cd build && \
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=~/lambda-install && \
make && sudo make install

# Install hiredis
rm -rf /tmp/hiredis-1.2.0 /tmp/hiredis-1.2.0.tar.gz

wget -O /tmp/hiredis-1.2.0.tar.gz https://github.com/redis/hiredis/archive/refs/tags/v1.2.0.tar.gz
tar -xvf /tmp/hiredis-1.2.0.tar.gz -C /tmp
cd /tmp/hiredis-1.2.0
make USE_SSL=1
sudo make install
sudo make install-ssl
rm -rf /tmp/hiredis-1.2.0.tar.gz /tmp/hiredis-1.2.0

# Install GKlib and Metis
git clone https://github.com/KarypisLab/GKlib.git /tmp/GKlib
cd /tmp/GKlib
make config
make
sudo make install

git clone https://github.com/KarypisLab/METIS.git /tmp/METIS
cd /tmp/METIS
make config i64=1 r64=1 cc=gcc prefix=~/local
make
sudo make install

rm -rf /tmp/GKlib /tmp/METIS
