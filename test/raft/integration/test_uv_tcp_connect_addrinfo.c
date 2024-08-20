#include "../../../src/raft.h"
#include "../../../src/raft.h"
#include "../lib/addrinfo.h"
#include "../lib/heap.h"
#include "../lib/loop.h"
#include "../lib/runner.h"
#include "../lib/tcp.h"

/******************************************************************************
 *
 * Fixture with a TCP-based raft_uv_transport.
 *
 *****************************************************************************/

struct fixture
{
    FIXTURE_HEAP;
    FIXTURE_LOOP;
    FIXTURE_TCP_SERVER;
    struct raft_uv_transport transport;
    bool closed;
};

/******************************************************************************
 *
 * Helper macros
 *
 *****************************************************************************/

struct result
{
    int status;
    bool done;
};

static void closeCb(struct raft_uv_transport *transport)
{
    struct fixture *f = transport->data;
    f->closed = true;
}

static void connectCbAssertResult(struct raft_uv_connect *req,
                                  struct uv_stream_s *stream,
                                  int status)
{
    struct result *result = req->data;
    munit_assert_int(status, ==, result->status);
    if (status == 0) {
        uv_close((struct uv_handle_s *)stream, (uv_close_cb)raft_free);
    }
    result->done = true;
}

#define INIT                                                         \
    do {                                                             \
        int _rv;                                                     \
        _rv = f->transport.init(&f->transport, 1, "127.0.0.1:9000"); \
        munit_assert_int(_rv, ==, 0);                                \
        f->transport.data = f;                                       \
        f->closed = false;                                           \
    } while (0)

#define CLOSE_SUBMIT               \
    munit_assert_false(f->closed); \
    f->transport.close(&f->transport, closeCb);

#define CLOSE_WAIT LOOP_RUN_UNTIL(&f->closed)
#define CLOSE     \
    CLOSE_SUBMIT; \
    CLOSE_WAIT

#define CONNECT_REQ(ID, ADDRESS, RV, STATUS)                      \
    struct raft_uv_connect _req;                                  \
    struct result _result = {STATUS, false};                      \
    int _rv;                                                      \
    _req.data = &_result;                                         \
    _rv = f->transport.connect(&f->transport, &_req, ID, ADDRESS, \
                               connectCbAssertResult);            \
    munit_assert_int(_rv, ==, RV)

/* Try to submit a connect request and assert that the given error code and
 * message are returned. */
#define CONNECT_ERROR(ID, ADDRESS, RV, ERRMSG)                  \
    {                                                           \
        CONNECT_REQ(ID, ADDRESS, RV /* rv */, 0 /* status */);  \
        munit_assert_string_equal(f->transport.errmsg, ERRMSG); \
    }

/* Submit a connect request with the given parameters and wait for the operation
 * to successfully complete. */
#define CONNECT(ID, ADDRESS)                                  \
    {                                                         \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, 0 /* status */); \
        LOOP_RUN_UNTIL(&_result.done);                        \
    }

/* Submit a connect request with the given parameters and wait for the operation
 * to fail with the given code and message. */
#define CONNECT_FAILURE(ID, ADDRESS, STATUS, ERRMSG)            \
    {                                                           \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, STATUS);           \
        LOOP_RUN_UNTIL(&_result.done);                          \
        munit_assert_string_equal(f->transport.errmsg, ERRMSG); \
    }

/* Submit a connect request with the given parameters, close the transport after
 * N loop iterations and assert that the request got canceled. */
#define CONNECT_CLOSE(ID, ADDRESS, N)                        \
    {                                                        \
        CONNECT_REQ(ID, ADDRESS, 0 /* rv */, RAFT_CANCELED); \
        LOOP_RUN(N);                                         \
        CLOSE_SUBMIT;                                        \
        munit_assert_false(_result.done);                    \
        LOOP_RUN_UNTIL(&_result.done);                       \
        CLOSE_WAIT;                                          \
    }

/******************************************************************************
 *
 * Set up and tear down.
 *
 *****************************************************************************/

static void *setUpDeps(const MunitParameter params[],
                       MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    int rv;
    SET_UP_ADDRINFO;
    SET_UP_HEAP;
    SETUP_LOOP;
    SETUP_TCP_SERVER;
    f->transport.version = 1;
    rv = raft_uv_tcp_init(&f->transport, &f->loop);
    munit_assert_int(rv, ==, 0);
    return f;
}

static void tearDownDeps(void *data)
{
    struct fixture *f = data;
    LOOP_STOP;
    raft_uv_tcp_close(&f->transport);
    TEAR_DOWN_TCP_SERVER;
    TEAR_DOWN_LOOP;
    TEAR_DOWN_HEAP;
    TEAR_DOWN_ADDRINFO;
    free(f);
}

static void *setUp(const MunitParameter params[], void *user_data)
{
    struct fixture *f = setUpDeps(params, user_data);
    INIT;
    return f;
}

static void tearDown(void *data)
{
    struct fixture *f = data;
    CLOSE;
    tearDownDeps(f);
}

/******************************************************************************
 *
 * raft_uv_transport->connect()
 *
 *****************************************************************************/

#define BOGUS_ADDRESS "127.0.0.1:6666"
#define INVALID_ADDRESS "500.0.0.1:6666"

SUITE(tcp_connect)

/* Successfully connect to the peer by first IP  */
TEST(tcp_connect, firstIP, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    const struct AddrinfoResult results[] = {{"127.0.0.1", TCP_SERVER_PORT},
                                             {"192.0.2.0", 6666}};
    AddrinfoInjectSetResponse(0, 2, results);
    CONNECT(2, "any-host");
    return MUNIT_OK;
}

/* Successfully connect to the peer by second IP  */
TEST(tcp_connect, secondIP, setUp, tearDown, 0, NULL)
{
    struct fixture *f = data;
    const struct AddrinfoResult results[] = {{"127.0.0.1", .6666},
                                             {"127.0.0.1", TCP_SERVER_PORT}};

    AddrinfoInjectSetResponse(0, 2, results);
    CONNECT(2, "any-host");
    return MUNIT_OK;
}

static void checkCb(struct uv_check_s *check)
{
    struct fixture *f = check->data;
    CLOSE_SUBMIT;
    uv_close((struct uv_handle_s *)check, NULL);
}

/* The transport gets closed right after the first connection attempt failed,
 * while doing a second connection attempt. */
TEST(tcp_connect, closeDuringSecondConnect, setUp, tearDownDeps, 0, NULL)
{
    struct fixture *f = data;
    struct uv_check_s check;
    int rv;
    const struct AddrinfoResult results[] = {{"127.0.0.1", .6666},
                                             {"127.0.0.1", TCP_SERVER_PORT}};

    AddrinfoInjectSetResponse(0, 2, results);

    /* Use a check handle in order to close the transport in the same loop
     * iteration where the second connection attempt occurs. */
    rv = uv_check_init(&f->loop, &check);
    munit_assert_int(rv, ==, 0);
    check.data = f;
    CONNECT_REQ(2, "any-host", 0, RAFT_CANCELED);
    /* Successfull DNS lookup will initiate async connect */
    LOOP_RUN(1);
    uv_check_start(&check, checkCb);
    LOOP_RUN(1);
    LOOP_RUN_UNTIL(&_result.done);
    CLOSE_WAIT;
    return MUNIT_OK;
}
