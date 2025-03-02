# 1. Use a lightweight base image
FROM ubuntu:22.04

# 2. Set environment variable to suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# 3. Set aws credential
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# 4. Install necessary runtime dependencies
RUN apt-get update && apt-get install -y \
    git \
    g++ \
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
    && rm -rf /var/lib/apt/lists/*

# 5. Clone and install glog
RUN git clone https://github.com/google/glog.git /tmp/glog && \
    cd /tmp/glog && \
    cmake . && \
    make && make install && \
    rm -rf /tmp/glog

# 6. Install AWS SDK C++
RUN git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git /tmp/aws-sdk-cpp && \
    cd /tmp/aws-sdk-cpp && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_ONLY="s3;ecs;lambda" \
            -DCMAKE_BUILD_TYPE=Debug && \
    cmake --build . --config=Debug && \
    cmake --install . --config=Debug && \
    rm -rf /tmp/aws-sdk-cpp

# 7. Install hiredis
RUN wget -O /tmp/hiredis-1.2.0.tar.gz https://github.com/redis/hiredis/archive/refs/tags/v1.2.0.tar.gz && \
    tar -xvf /tmp/hiredis-1.2.0.tar.gz -C /tmp && \
    cd /tmp/hiredis-1.2.0 && \
    make USE_SSL=1 && make install && make install-ssl && \
    rm -rf /tmp/hiredis-1.2.0.tar.gz /tmp/hiredis-1.2.0

# 8. Create the working directory
WORKDIR /app

# 9. Copy the compiled proxy_server executable
COPY ./build/proxy_server /app/proxy_server

# 10. Make sure the executable is runnable
RUN chmod +x /app/proxy_server

# 11. Set the command to run the proxy_server
CMD ["./proxy_server", "--elastic-proxy", "--elasticache-host", "faasboard-hcdnu5.serverless.apse1.cache.amazonaws.com", "-cores", "16", "--v", "1"]