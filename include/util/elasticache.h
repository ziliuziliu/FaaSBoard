#ifndef REDIS_SERVERLESS_CLIENT_H
#define REDIS_SERVERLESS_CLIENT_H

#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>
#include <string>
#include <iostream>

class ElastiCache {
private:
    redisContext* redisCtx;
    std::string host;
    int port;
    bool isCluster;

public:
    ElastiCache(const std::string& _host, int _port) 
        : redisCtx(nullptr)
        , host(_host)
        , port(_port)
        , isCluster(false) {
    }

    ~ElastiCache() {
        if (redisCtx) {
            redisFree(redisCtx);
        }
    }

    bool connect() {
        // Connect to Redis
        redisCtx = redisConnect(host.c_str(), port);
        if (redisCtx == nullptr || redisCtx->err) {
            std::cerr << "Failed to connect to Redis: "
                      << (redisCtx ? redisCtx->errstr : "unknown error") << std::endl;
            return false;
        }

        // Set up SSL/TLS
        redisSSLContext* sslContext = redisCreateSSLContext(
            nullptr,  // cert file
            nullptr,  // key file
            nullptr,  // ca cert
            nullptr,  // ca path
            nullptr,  // server name
            nullptr   // error
        );

        if (!sslContext) {
            std::cerr << "Failed to create SSL context" << std::endl;
            redisFree(redisCtx);
            redisCtx = nullptr;
            return false;
        }

        if (redisInitiateSSLWithContext(redisCtx, sslContext) != REDIS_OK) {
            std::cerr << "Failed to establish SSL connection: " << redisCtx->errstr << std::endl;
            redisFree(redisCtx);
            redisCtx = nullptr;
            redisFreeSSLContext(sslContext);
            return false;
        }

        redisFreeSSLContext(sslContext);

        // Check if it's a cluster
        redisReply* reply = (redisReply*)redisCommand(redisCtx, "CLUSTER INFO");
        if (reply && reply->type != REDIS_REPLY_ERROR) {
            isCluster = true;
        }
        if (reply) freeReplyObject(reply);

        return true;
    }

    bool setnx(const std::string& key, const std::string& value) {
        if (!redisCtx) {
            std::cerr << "Redis context is not initialized." << std::endl;
            return false;
        }

        redisReply* reply = (redisReply*)redisCommand(redisCtx, "SETNX %s %s", 
                                                    key.c_str(), value.c_str());
        if (!reply) {
            std::cerr << "SETNX command failed." << std::endl;
            return false;
        }

        // Redis returns 1 if the key was set, 0 if it already exists
        bool success = (reply->type == REDIS_REPLY_INTEGER && reply->integer == 1);
        
        freeReplyObject(reply);
        return success;
    }

    std::string get(const std::string& key) {
        if (!redisCtx) {
            std::cerr << "Redis context is not initialized." << std::endl;
            return "";
        }

        redisReply* reply = (redisReply*)redisCommand(redisCtx, "GET %s", key.c_str());
        if (!reply || reply->type != REDIS_REPLY_STRING) {
            std::cerr << "GET command failed or key not found." << std::endl;
            if (reply) freeReplyObject(reply);
            return "";
        }

        std::string value = reply->str;
        freeReplyObject(reply);
        return value;
    }

    void close() {
        if (redisCtx) {
            redisFree(redisCtx);
            redisCtx = nullptr;
        }
    }
};

#endif // REDIS_SERVERLESS_CLIENT_H