# frozen_string_literal: true

require "forwardable"
require "concurrent-ruby"

module Vessel
  class Scheduler
    extend Forwardable
    delegate %i[scheduled_task_count completed_task_count queue_length] => :pool

    attr_reader :browser, :queue, :delay, :headers

    def initialize(queue, settings)
      @queue = queue
      @min_threads, @max_threads, @delay, @headers, @intercept =
        settings.values_at(:min_threads, :max_threads, :delay, :headers, :intercept)

      options = settings[:ferrum]
      options.merge!(timeout: settings[:timeout]) if settings[:timeout]
      @browser = Ferrum::Browser.new(**options)
    end

    def post(*requests)
      requests.map do |request|
        Concurrent::Promises.future_on(pool, queue, request) do |queue, request|
          queue << goto(request)
        end
      end
    end

    def stop
      pool.shutdown
      pool.kill unless pool.wait_for_termination(30)
      browser.quit
    end

    private

    def pool
      @pool ||= Concurrent::ThreadPoolExecutor.new(
        max_queue: 0,
        min_threads: @min_threads,
        max_threads: @max_threads
      )
    end

    def goto(request)
      return [nil, request] if request.stub?

      page = browser.create_page
      page.headers.set(headers) if headers
      if @intercept
        page.network.intercept
        page.on(:request, &@intercept)
      end

      # Delay is set between requests when we don't want to bombard server with
      # requests so it requires crawler to be single threaded. Otherwise doesn't
      # make sense.
      sleep(delay) if @max_threads == 1 && delay > 0
      page.goto(request.url)
      [page, request]
    rescue => e
      [page, request, e]
    end
  end
end
