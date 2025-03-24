#ifndef _TYPES_H
#define _TYPES_H

#define CAAS_SPARSE_LIMIT 1000000
#define CAAS_SPARSE_PAIR_LIMIT 100000

#define CAAS_META_SERVER_PORT 20000

#define CAAS_KILL_MESSAGE 0xffffffff

#define COMM_COMP_RATIO 10

#define FARGATE_CLUSTER "faasboard"
#define FARGATE_SERVICE "proxy_server"

enum class RUN_TYPE : unsigned int {
    LOCAL = 0,
    LAMBDA = 1,
};

enum class EDGE_DIRECTION : unsigned int {
    INCOMING = 0,
    OUTGOING = 1,
};

enum class WORKER_STATUS : unsigned int {
    WORKING = 0,
    STEALING = 1,
};

enum class CAAS_TYPE : unsigned int {
    UINT32 = 0,
    INT = 1,
    FLOAT = 2,
    DEFAULT = 3,
};

enum class CAAS_OP : unsigned int {
    MASKED_BROADCAST = 0,
    MASKED_REDUCE = 1,
    ALLREDUCE = 2,
};

enum class CAAS_REDUCE_OP : unsigned int {
    UP = 0,
    ADD = 1,
    MIN = 2,
};

enum class CAAS_COMM_MODE : unsigned int {
    PROXY = 0,
    S3 = 1,
    DIRECT = 2,
};

enum class COMM_TYPE : unsigned int {
    CAAS_MAGIC = 0,
    CAAS_PAIR = 1,
    CAAS_SPARSE = 2,
    CAAS_DENSE = 3,
};

enum class CAAS_SAVE_MODE : unsigned int {
    NO_SAVE = 0,
    SAVE_LOCAL = 1,
    SAVE_S3 = 2,
};

enum class REQUEST_EXECUTION_STATUS : unsigned int {
    INIT = 0,
    EXECUTE = 1,
    REINVOKE = 2,
};

typedef void * empty;

#endif