# frozen_string_literal: true

require "test_helper"
require "uri"

class NativeLibraryTest < Minitest::Test
  def setup
    @saved = ENV.fetch("STAMPEDED_INSTALL_DIR", nil)
  end

  def teardown
    ENV["STAMPEDED_INSTALL_DIR"] = @saved
  end

  def test_a_wrong_install_dir_names_the_path_it_looked_at
    ENV["STAMPEDED_INSTALL_DIR"] = "/nonexistent/path"
    error = assert_raises(Stampeded::LibraryNotFound) { Stampeded::Native.library_path }
    assert_includes error.message, "/nonexistent/path/#{Stampeded::Native::LIBRARY}"
  end

  def test_without_an_install_dir_the_error_says_how_to_build_one
    ENV.delete("STAMPEDED_INSTALL_DIR")
    skip "this checkout has a staged library" if File.file?(File.expand_path("../libexec/#{Stampeded::Native::LIBRARY}", __dir__))

    error = assert_raises(Stampeded::LibraryNotFound) { Stampeded::Native.library_path }
    assert_includes error.message, "cargo build --release -p stampeded-ffi"
    assert_includes error.message, "STAMPEDED_INSTALL_DIR"
  end

  def test_a_refused_connect_says_so_once_with_the_reason
    error = assert_raises(Stampeded::Error) do
      Stampeded::Native::Puller.new(url: "postgres://nobody@127.0.0.1:1/none", queues: "default", except: nil,
                                    max_leases: 1, batch_size: 1, poll_interval: 0.05, heartbeat_interval: 30)
    end
    assert_equal 1, error.message.scan("could not connect").size, error.message
    assert_match(/connection refused/i, error.message)
  end
end

class NativeWorkerTest < Minitest::Test
  include StampededTest::Isolation

  def test_connection_params_become_a_url_the_library_accepts
    role = "rb_#{SecureRandom.hex(4)}"
    password = "p@ss:w/rd?&=%#+ ok"
    conn do |c|
      c.exec("CREATE ROLE #{role} LOGIN PASSWORD #{c.escape_literal(password)}")
      c.exec("GRANT USAGE ON SCHEMA #{@schema} TO #{role}")
      c.exec("GRANT ALL ON ALL TABLES IN SCHEMA #{@schema} TO #{role}")
      c.exec("GRANT ALL ON ALL SEQUENCES IN SCHEMA #{@schema} TO #{role}")
      c.exec("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA #{@schema} TO #{role}")
    end

    base = URI(StampededTest::URL)
    url = Stampeded::Connection.url(host: base.host, port: base.port, dbname: base.path.delete_prefix("/"),
                                    user: role, password: password, options: "-csearch_path=#{@schema}")
    puller = Stampeded::Native::Puller.new(url: url, queues: "default", except: nil, max_leases: 1,
                                           batch_size: 1, poll_interval: 0.05, heartbeat_interval: 30)
    registered = conn { |c| c.exec_params("SELECT count(*) FROM stampeded_workers WHERE id = $1", [puller.worker_id])[0]["count"] }
    assert_equal "1", registered
    puller.stop(0)
  ensure
    puller&.close
    conn { |c| c.exec("DROP OWNED BY #{role}; DROP ROLE IF EXISTS #{role}") } if role
  end

  def test_a_worker_short_of_disk_claims_nothing_and_says_why_on_its_row
    # 100% free never holds on a real disk: the first probe pauses claiming.
    Stampeded.config.vitals = { disks: [{ path: Dir.tmpdir, min_free: "100%" }], probe_interval: 0.05 }
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("held") { nil }
    id = Stampeded.enqueue(queue: "default", task: "held")
    thread = Thread.new { worker.run }

    vitals = -> { conn { |c| c.exec("SELECT metadata->'vitals' AS v FROM stampeded_workers").first&.fetch("v") } }
    deadline = Time.now + 5
    sleep 0.05 until vitals.() || Time.now > deadline
    row = JSON.parse(vitals.() || "null")
    sleep 0.3

    refute_nil row, "the worker row never recorded its vitals"
    assert_equal false, row["admitting"]
    assert_equal "warning", row["phase"]
    assert_equal "disk:#{Dir.tmpdir}", row["vital"]
    assert_equal "available", job_row(id)["state"], "claimed while short of disk"
  ensure
    worker&.stop
    thread&.join(5)
    Stampeded.config.vitals = nil
  end

  def test_room_on_disk_claims_as_usual
    puller = Stampeded::Native::Puller.new(url: Stampeded::Connection.url, queues: "default", except: nil,
                                           max_leases: 1, batch_size: 1, poll_interval: 0.05, heartbeat_interval: 30,
                                           vitals: { disks: [{ path: Dir.tmpdir, min_free: 1 }] })
    id = Stampeded.enqueue(queue: "default", task: "t")
    lease = puller.next(2)
    assert_equal id, lease&.row&.fetch("id")
    lease.complete!
    puller.stop(0)
  ensure
    puller&.close
  end

  def test_an_unusable_vitals_config_is_refused_with_the_reason
    error = assert_raises(Stampeded::Error) do
      Stampeded::Native::Puller.new(url: Stampeded::Connection.url, queues: "default", except: nil,
                                    max_leases: 1, batch_size: 1, poll_interval: 0.05, heartbeat_interval: 30,
                                    vitals: { disks: [{ path: "relative", min_free: 1 }] })
    end
    assert_includes error.message, "vitals disk path relative must be absolute"
    assert_equal 0, conn { |c| c.exec("SELECT count(*) FROM stampeded_workers")[0]["count"].to_i }
  end

  def test_a_worker_that_dies_stops_claiming_and_deregisters
    worker = Stampeded::Worker.new(queue: "default", poll_interval: 0.05)
    worker.register("late") { nil }
    thread = Thread.new { worker.run }
    workers = -> { conn { |c| c.exec("SELECT count(*) FROM stampeded_workers")[0]["count"].to_i } }
    deadline = Time.now + 5
    sleep 0.05 until workers.() == 1 || Time.now > deadline
    assert_equal 1, workers.(), "the worker never registered"

    thread.kill
    thread.join(5)
    id = Stampeded.enqueue(queue: "default", task: "late")
    sleep 0.5

    assert_equal "available", job_row(id)["state"], "a dead worker's pool went on claiming"
    assert_equal 0, workers.(), "a dead worker left its registration behind"
  ensure
    thread&.kill
  end
end
