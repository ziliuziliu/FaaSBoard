# 1. Use a lightweight base image
FROM ubuntu:22.04

# 2. Set environment variable to suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# 3. Install necessary runtime dependencies
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
    && rm -rf /var/lib/apt/lists/*

# 4. Clone and install glog
RUN git clone https://github.com/google/glog.git /tmp/glog && \
    cd /tmp/glog && \
    cmake . && \
    make && make install && \
    rm -rf /tmp/glog

# 5. Install AWS SDK C++
RUN git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git /tmp/aws-sdk-cpp && \
    cd /tmp/aws-sdk-cpp && \
    mkdir build && \
    cd build && \
    cmake .. -DBUILD_ONLY="ecs" \
            -DCMAKE_BUILD_TYPE=Release \
            -DBUILD_SHARED_LIBS=ON && \
    make && \
    make install && \
    rm -rf /tmp/aws-sdk-cpp && \
    ldconfig

# 6. Create the working directory
WORKDIR /app

# 7. Copy the compiled proxy_server executable
COPY ./build/proxy_server /app/proxy_server

# 8. Make sure the executable is runnable
RUN chmod +x /app/proxy_server

# 9. Set the command to run the proxy_server
CMD ["./proxy_server", "-cores", "4", "--v", "1"]