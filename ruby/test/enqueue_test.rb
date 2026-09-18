# frozen_string_literal: true

require "test_helper"

class EnqueueTest < Minitest::Test
  include StampedeTest::Isolation

  def test_enqueue_returns_the_row_id
    id = Stampede.enqueue(queue: "default", task: "greet", args: { who: "ruby" })

    refute_nil id
    row = job_row(id)
    assert_equal "greet", row["task"]
    assert_equal "available", row["state"]
    assert_equal({ "who" => "ruby" }, JSON.parse(row["args"]))
  end

  def test_every_column_the_row_has_is_reachable
    # The Rust HTTP API shipped silently dropping three of these, so the gem
    # gets the same test: a field the caller sets must actually land.
    id = Stampede.enqueue(
      queue: "default", task: "sync",
      destination: "api.vendor.com", dead_letter_queue: "dead",
      expire_seconds: 42, priority: 3, max_attempts: 7, tenant_id: "acme"
    )

    row = job_row(id)
    assert_equal "api.vendor.com", row["destination"]
    assert_equal "dead", row["dead_letter_queue"]
    assert_equal "42", row["expire_seconds"]
    assert_equal "3", row["priority"]
    assert_equal "7", row["max_attempts"]
    assert_equal "acme", row["tenant_id"]
  end

  def test_a_deduplicated_job_returns_nil_rather_than_raising
    # The caller's intent -- ensure this job exists -- was satisfied. Making it
    # an error forces every caller to tell "already queued" from "failed".
    first = Stampede.enqueue(queue: "default", task: "report", unique: "daily")
    second = Stampede.enqueue(queue: "default", task: "report", unique: "daily")

    refute_nil first
    assert_nil second
  end

  def test_a_period_bucket_is_one_per_window_not_one_ever
    a = Stampede.enqueue(queue: "default", task: "r", unique: "k", unique_period: "hour")
    b = Stampede.enqueue(queue: "default", task: "r", unique: "k", unique_period: "hour")
    c = Stampede.enqueue(queue: "default", task: "r", unique: "k", unique_period: "day")

    refute_nil a
    assert_nil b, "same scope, same hour"
    refute_nil c, "a different window is a different key"
  end

  def test_depth_reads_counters_not_an_aggregate
    3.times { Stampede.enqueue(queue: "default", task: "x") }

    assert_equal({ "available" => 3 }, Stampede.depth("default"))
  end
end
