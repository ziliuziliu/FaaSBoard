#ifndef _TYPES_H
#define _TYPES_H

#define INCOMING 0
#define OUTGOING 1

#define WORKING 0
#define STEALING 1

#define CAAS_UINT32 0
#define CAAS_INT 1
#define CAAS_FLOAT 2

#define CAAS_MASKED_BROADCAST 0
#define CAAS_MASKED_REDUCE 1
#define CAAS_ALLREDUCE 2

#define CAAS_UP 0
#define CAAS_ADD 1
#define CAAS_MIN 2

#define CAAS_PROXY 0
#define CAAS_S3 1
#define CAAS_DIRECT 2

enum class COMM_TYPE : int {
    CAAS_MAGIC = 0,
    CAAS_PAIR = 1,
    CAAS_SPARSE = 2,
    CAAS_DENSE = 3,
};

#define CAAS_FD_NOTINQUEUE 0
#define CAAS_FD_INQUEUE 1

#define CAAS_SPARSE_LIMIT 1000000
#define CAAS_SPARSE_PAIR_LIMIT 100000

typedef void * empty;

#endif