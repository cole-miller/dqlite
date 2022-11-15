#ifndef DQLITE_PROTOCOL_H_
#define DQLITE_PROTOCOL_H_

/* Special datatypes */
#define DQLITE_UNIXTIME 9
#define DQLITE_ISO8601 10
#define DQLITE_BOOLEAN 11

#define DQLITE_PROTO 1001 /* Protocol error */

/* Role codes */
enum {
    DQLITE_VOTER,
    DQLITE_STANDBY,
    DQLITE_SPARE
};

/* Current protocol version */
#define DQLITE_PROTOCOL_VERSION 1

/* Legacly pre-1.0 version. */
#define DQLITE_PROTOCOL_VERSION_LEGACY 0x86104dd760433fe5

/* Special value indicating that a batch of rows is over, but there are more. */
#define DQLITE_RESPONSE_ROWS_PART 0xeeeeeeeeeeeeeeee

/* Special value indicating that the result set is complete. */
#define DQLITE_RESPONSE_ROWS_DONE 0xffffffffffffffff

/* Request types */
enum {
    DQLITE_REQUEST_LEADER,
    DQLITE_REQUEST_CLIENT,
    DQLITE_REQUEST_HEARTBEAT,
    DQLITE_REQUEST_OPEN,
    DQLITE_REQUEST_PREPARE,
    DQLITE_REQUEST_EXEC,
    DQLITE_REQUEST_QUERY,
    DQLITE_REQUEST_FINALIZE,
    DQLITE_REQUEST_EXEC_SQL,
    DQLITE_REQUEST_QUERY_SQL,
    DQLITE_REQUEST_INTERRUPT,
    DQLITE_REQUEST_CONNECT,
    DQLITE_REQUEST_ADD,
    DQLITE_REQUEST_ASSIGN,
    DQLITE_REQUEST_REMOVE,
    DQLITE_REQUEST_DUMP,
    DQLITE_REQUEST_CLUSTER,
    DQLITE_REQUEST_TRANSFER,
    DQLITE_REQUEST_DESCRIBE,
    DQLITE_REQUEST_WEIGHT,
    DQLITE_REQUEST_DROP
};

#define DQLITE_REQUEST_CLUSTER_FORMAT_V0 0 /* ID and address */
#define DQLITE_REQUEST_CLUSTER_FORMAT_V1 1 /* ID, address and role */

#define DQLITE_REQUEST_DESCRIBE_FORMAT_V0 0 /* Failure domain and weight */

/* Response types */
enum {
    DQLITE_RESPONSE_FAILURE,
    DQLITE_RESPONSE_SERVER,
    DQLITE_RESPONSE_SERVER_LEGACY = DQLITE_RESPONSE_SERVER,
    DQLITE_RESPONSE_WELCOME,
    DQLITE_RESPONSE_SERVERS,
    DQLITE_RESPONSE_DB,
    DQLITE_RESPONSE_STMT,
    DQLITE_RESPONSE_RESULT,
    DQLITE_RESPONSE_ROWS,
    DQLITE_RESPONSE_EMPTY,
    DQLITE_RESPONSE_FILES,
    DQLITE_RESPONSE_METADATA
};

#endif /* DQLITE_PROTOCOL_H_ */
