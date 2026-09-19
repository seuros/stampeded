# frozen_string_literal: true

require "state_machines"

module Stampeded
  # The job lifecycle, declared once.
  #
  # This mirrors `crates/stampeded/src/lifecycle.rs` — same states, same events,
  # same transitions — so the two first-party workers cannot disagree about what
  # a job is allowed to do. Until this existed the Ruby side kept the state as a
  # bare string and compared it against literals, which is precisely what
  # plans/02 criticises every surveyed implementation for:
  #
  # > Every reference implementation stores this as a bare string column and
  # > compares it with string literals scattered across the codebase.
  #
  # The legal transitions were then not written down anywhere in Ruby — they
  # were whatever the set of comparisons happened to permit, which is how
  # `executing → available` (losing an attempt) or `completed → retryable`
  # (running a finished job again) get added by accident.
  #
  # **SQL REMAINS AUTHORITATIVE.** A polyglot worker calls the SQL functions
  # directly and never loads this file, so the transitions in
  # `schema.sql` are what actually happen. This is the Ruby side's
  # statement of what it BELIEVES those functions will do — used for
  # predicates, for guarding operator actions before a round trip, and for
  # noticing when the two have drifted.
  class Lifecycle
    STATES = %i[available claimed executing retryable completed discarded cancelled].freeze

    attr_accessor :state

    def initialize(state = :available)
      @state = state.to_sym
    end

    state_machine :state, initial: :available do
      # Terminal states never transition again and are eligible for pruning.
      state :completed, :discarded, :cancelled do
        def terminal? = true
      end

      state :available, :claimed, :executing, :retryable do
        def terminal? = false
      end

      # A worker took it. `attempt` is incremented HERE, not at failure --
      # the plans/04 poison brake.
      event :claim do
        transition available: :executing
        # A retryable job whose backoff has elapsed is claimable. Leaving this
        # out is the poison-job bug: a crashed job parks in retryable, is never
        # re-claimed, never spends its attempt budget, and never reaches
        # max_attempts.
        transition retryable: :executing
      end

      event :complete do
        transition executing: :completed
      end

      # Failed with budget left.
      event :retry do
        transition executing: :retryable
      end

      # Failed with nothing left, or unrecoverable.
      event :discard do
        transition %i[executing retryable] => :discarded
      end

      # Operator action. Deliberately NOT permitted from `executing`: that job
      # belongs to a worker, and flipping the row underneath it would not stop
      # the work -- it would only make the worker's completion fail its
      # ownership check, turning a cancel into a silent no-op with extra steps.
      event :cancel do
        transition %i[available retryable] => :cancelled
      end

      # The reaper reclaiming a job from a dead worker.
      event :reclaim do
        transition %i[claimed executing] => :retryable
      end
    end

    # States a worker may claim from.
    def claimable? = can_claim?

    class << self
      # Is this transition legal, without performing it?
      def legal?(from, event)
        new(from).public_send(:"can_#{event}?")
      rescue IndexError, NoMethodError
        false
      end

      # What a job in this state may do next. Surfaced so an operator -- or a
      # model, through MCP -- can be told what is allowed rather than trying
      # something and getting a refusal.
      def events_from(state)
        new(state).state_transitions.map(&:event)
      rescue IndexError
        []
      end

      def terminal?(state) = new(state).terminal?

      def claimable?(state) = legal?(state, :claim)

      # Check what SQL actually did against the declared lifecycle.
      #
      # It cannot PREVENT anything -- the SQL functions are authoritative and a
      # polyglot worker never runs this code. It exists to make drift loud: if
      # a WHERE clause is edited and starts permitting a transition this file
      # forbids, the alternative to a warning is finding out from a customer.
      def verify(from, to, event)
        machine = new(from)
        return true unless machine.respond_to?(:"can_#{event}?")

        machine.public_send(:"#{event}")
        machine.state.to_s == to.to_s
      rescue StandardError
        false
      end
    end
  end
end
