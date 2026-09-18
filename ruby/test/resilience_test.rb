# frozen_string_literal: true

require "test_helper"

RECOVERED = Queue.new

class ResilienceTest < Minitest::Test
  include StampededTest::Isolation

  def setup
    super
    RECOVERED.clear
  end

  def test_a_worker_reconnects_after_the_server_closes_its_connection
    # THE BUG THIS EXISTS FOR: the claim loop rescued PG::Error, slept, and
    # retried on the SAME DEAD CONNECTION -- forever. The thread stayed alive
    # and the process stayed up, so every liveness check passed while the
    # worker silently did nothing. Any failover, pooler restart, admin
    # pg_terminate_backend or network blip wedged it permanently.
    #
    # The Rust worker never had this: sqlx's pool discards broken connections
    # and opens new ones. Ruby manages a raw connection by hand, so it has to
    # do it explicitly.
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.2)
    worker.register("x") { RECOVERED << :ran }
    thread = Thread.new { worker.run }

    Stampeded.enqueue(queue: "default", task: "x")
    wait_until { RECOVERED.size >= 1 }
    before = RECOVERED.size
    assert_operator before, :>=, 1, "the worker must be working before we break it"

    killed = kill_worker_backends
    assert_operator killed, :>=, 1, "the test must actually kill something"

    # A job enqueued AFTER the kill. Seeing it requires a live connection.
    sleep 0.5
    Stampeded.enqueue(queue: "default", task: "x")
    wait_until(timeout: 15) { RECOVERED.size > before }

    assert_operator RECOVERED.size, :>, before,
                    "the worker ran nothing after its connection was killed"
    assert thread.alive?, "and it should not have died either"
  ensure
    worker&.stop
    thread&.join(5)
    thread&.kill
  end

  private

  def wait_until(timeout: 10)
    deadline = Time.now + timeout
    sleep 0.05 until yield || Time.now > deadline
  end

  # Everything on this database except our own admin connection. The worker's
  # connections are the only other ones, since each test gets its own schema.
  def kill_worker_backends
    admin = PG.connect(StampededTest::URL)
    n = admin.exec(<<~SQL)[0]["count"].to_i
      SELECT count(*) FROM (
        SELECT pg_terminate_backend(pid) FROM pg_stat_activity
         WHERE datname = current_database()
           AND pid <> pg_backend_pid()
           AND application_name NOT LIKE '%psql%'
      ) x
    SQL
    admin.close
    n
  end
end
