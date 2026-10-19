/* The C ABI end to end, in C23; tests/c_abi.rs builds the database and checks the rows. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "stampeded.h"

constexpr unsigned WAIT_MS = 3000;

static int failures = 0;

#define CHECK(cond, ...)                         \
  do {                                           \
    if (!(cond)) {                               \
      fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
      fprintf(stderr, __VA_ARGS__);              \
      fprintf(stderr, " (last error: %s)\n",     \
              stampeded_last_error() ? stampeded_last_error() : "none"); \
      failures++;                                \
    }                                            \
  } while (0)

static stampeded_lease *next(stampeded_puller *puller) {
  stampeded_lease *lease = nullptr;
  int32_t status = stampeded_puller_next(puller, WAIT_MS, &lease);
  CHECK(status == STAMPEDED_OK, "puller_next returned %d", status);
  return status == STAMPEDED_OK ? lease : nullptr;
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s <database-url>\n", argv[0]);
    return 2;
  }
  printf("stampeded %s\n", stampeded_version());

  CHECK(stampeded_log_init("stampeded=loud") == STAMPEDED_ERROR, "a bad log filter is refused");
  CHECK(stampeded_log_init("warn") == STAMPEDED_OK, "log_init");
  CHECK(stampeded_log_init(nullptr) == STAMPEDED_NONE, "a second logger is not installed");

  stampeded_client *client = stampeded_connect(argv[1]);
  if (client == nullptr) {
    fprintf(stderr, "connect failed: %s\n", stampeded_last_error());
    return 1;
  }

  char id[37] = {0};
  CHECK(stampeded_enqueue(client, "ffi", "greet", "{\"name\":\"c23\"}", nullptr, id) == STAMPEDED_OK,
        "enqueue greet");
  CHECK(strlen(id) == 36, "id is a UUID, got \"%s\"", id);

  stampeded_enqueue_options once = {.unique = "only-once", .meta_json = "{\"c\":{\"std\":23}}"};
  CHECK(stampeded_enqueue(client, "ffi", "flaky", "{}", &once, nullptr) == STAMPEDED_OK, "enqueue flaky");
  CHECK(stampeded_enqueue(client, "ffi", "flaky", "{}", &once, nullptr) == STAMPEDED_NONE,
        "a second unique job is refused while the first waits");

  CHECK(stampeded_enqueue(client, "ffi", "bad", "{not json", nullptr, nullptr) == STAMPEDED_ERROR,
        "invalid JSON is refused");
  CHECK(stampeded_last_error() != nullptr && strstr(stampeded_last_error(), "JSON") != nullptr,
        "the error names the JSON");
  CHECK(stampeded_connect(nullptr) == nullptr, "a NULL url is refused");

  stampeded_puller_options options = {
      .queues = "ffi", .max_leases = 4, .poll_interval_ms = 50, .heartbeat_interval_ms = 1000};
  stampeded_puller *puller = stampeded_puller_start(client, &options);
  CHECK(puller != nullptr, "puller_start");
  if (puller == nullptr) return 1;
  CHECK(strlen(stampeded_puller_worker_id(puller)) == 36, "worker id: %s", stampeded_puller_worker_id(puller));

  for (int i = 0; i < 2; i++) {
    stampeded_lease *lease = next(puller);
    if (lease == nullptr) continue;

    bool greet = strcmp(stampeded_lease_task(lease), "greet") == 0;
    CHECK(strcmp(stampeded_lease_queue(lease), "ffi") == 0, "queue");
    CHECK(stampeded_lease_attempt(lease) == 1, "first attempt");

    if (greet) {
      CHECK(strcmp(stampeded_lease_id(lease), id) == 0, "the id enqueue returned");
      CHECK(strstr(stampeded_lease_args(lease), "\"c23\"") != nullptr, "args: %s", stampeded_lease_args(lease));
      const char *job = stampeded_lease_job(lease);
      CHECK(strstr(job, "\"state\":\"executing\"") != nullptr && strstr(job, stampeded_puller_worker_id(puller)) != nullptr,
            "the row is executing, claimed by this puller: %s", job);
      CHECK(stampeded_lease_checkpoint(lease, "{\"cursor\":1}") == STAMPEDED_OK, "checkpoint");
      CHECK(stampeded_lease_complete(lease) == STAMPEDED_OK, "complete");
    } else {
      CHECK(strstr(stampeded_lease_meta(lease), "\"std\":23") != nullptr, "meta: %s", stampeded_lease_meta(lease));
      CHECK(stampeded_lease_fail(lease, "downstream said no", 600000) == STAMPEDED_OK, "fail");
    }
  }

  CHECK(stampeded_enqueue(client, "ffi", "doomed", "{}", nullptr, nullptr) == STAMPEDED_OK, "enqueue doomed");
  stampeded_lease *doomed = next(puller);
  if (doomed != nullptr) CHECK(stampeded_lease_discard(doomed, "will never work") == STAMPEDED_OK, "discard");

  CHECK(stampeded_enqueue(client, "ffi", "abandoned", "{}", nullptr, nullptr) == STAMPEDED_OK, "enqueue abandoned");
  stampeded_lease *abandoned = next(puller);
  stampeded_lease_free(abandoned);
  stampeded_lease *again = next(puller);
  if (again != nullptr) {
    CHECK(strcmp(stampeded_lease_task(again), "abandoned") == 0, "the abandoned job comes back");
    CHECK(stampeded_lease_complete(again) == STAMPEDED_OK, "complete the abandoned job");
  }

  stampeded_lease *none = nullptr;
  CHECK(stampeded_puller_next(puller, 200, &none) == STAMPEDED_NONE && none == nullptr, "an empty queue");

  /* Timed out goes last: its backoff can be under a second, and nothing pulls after it. */
  CHECK(stampeded_enqueue(client, "ffi", "slow", "{}", nullptr, nullptr) == STAMPEDED_OK, "enqueue slow");
  stampeded_lease *slow = next(puller);
  if (slow != nullptr) CHECK(stampeded_lease_timed_out(slow, "ran past 1s") == STAMPEDED_OK, "timed_out");

  CHECK(stampeded_puller_stop(puller, 1000) == STAMPEDED_OK, "stop");
  stampeded_puller_free(puller);
  stampeded_client_free(client);

  if (failures) {
    fprintf(stderr, "%d check(s) failed\n", failures);
    return 1;
  }
  printf("ok\n");
  return 0;
}
