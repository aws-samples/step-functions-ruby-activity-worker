#
# Copyright 2017-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#

require 'set'
require 'json'
require 'thread'
require 'logger'
require 'aws-sdk-core'
require 'aws-sdk-states'

# Utility module for validating the input parameters.
module Validate
  # Validates if the input argument is positive integer.
  # @param value [Integer] the number to validate
  def self.positive(value)
    raise ArgumentError, 'Argument has to positive' if value <= 0
    value
  end

  # Validates if the input argument is not nil.
  # @param value [Integer] the number to validate
  def self.required(value)
    raise ArgumentError, 'Argument is required' if value.nil?
    value
  end
end

# Defines the Step Function related types.
module StepFunctions
  # Raised whenever a operation had failed after all retries attempts.
  class RetryError < StandardError
    # Creates new instance of RetryError.
    # @param message [String] the error message
    def initialize(message)
      super(message)
    end
  end

  # Executes the block with the configured number of retries.
  # @param options [Object] the settings
  # @param block [Block] the code to execute
  def self.with_retries(options = {}, &block)
    retries = 0
    base_delay_seconds = options[:base_delay_seconds] || 2
    max_retries = options[:max_retries] || 3
    begin
      block.call
    rescue => e
      puts e
      if retries < max_retries
        retries += 1
        sleep base_delay_seconds**retries
        retry
      end
      raise RetryError, 'All retries of operation had failed'
    end
  end

  # Activity - encapsulate the actual business logic of the state machine step.
  # The implementation hides all of the details of communicating with StepFunctions API like polling for activity,
  # sending heartbeats and returning the activity execution result. All of the operations are being performed with
  # configured number of retries.
  #
  # This is implementation of multiple producer - multiple consumer pattern. The are two dedicated thread polls, first
  # for polling for activity task, the second one for executing the business logic. Both thread polls size can be
  # configured using pollers_count and workers_count attributes.
  #
  # The execution is started upon calling start method. It's single argument is the code block that is going to be
  # executed by each activity worker thread. The code block single argument is the unmarshalled JSON passed as input
  # to the activity execution. The block should return as a output an object.
  #
  # The activity handles graceful shutdown. Whenever the process will be stopped it will stop polling for new task and
  # await for termination of the activities that are currently running.
  #
  # Example: echo activity - that returns the 'value' attribute as a output.
  #
  # activity = StepFunctions::Activity.new(
  # credentials: credentials,
  #     region: region,
  #     activity_arn: activity_arn,
  #     workers_count: 1,
  #     pollers_count: 1,
  #     heartbeat_delay: 30
  # )
  # activity.start do |input|
  #   { echo: input['value'] }
  # end
  class Activity
    # Creates new instance of the activity.
    # @param options [Object] the activity settings
    def initialize(options = {})
      @states = Aws::States::Client.new(
        credentials: Validate.required(options[:credentials]),
        region: Validate.required(options[:region]),
        http_read_timeout: Validate.positive(options[:http_read_timeout] || 60)
      )
      @activity_arn = Validate.required(options[:activity_arn])
      @heartbeat_delay = Validate.positive(options[:heartbeat_delay] || 60)
      @queue_max = Validate.positive(options[:queue_max] || 5)
      @pollers_count = Validate.positive(options[:pollers_count] || 1)
      @workers_count = Validate.positive(options[:workers_count] || 1)
      @max_retry = Validate.positive(options[:workers_count] || 3)
      @logger = Logger.new(STDOUT)
    end

    # Starts the execution of the activity.
    # This is a main entry point. Once this method will be called the executing thread will be blocked and dedicated
    # thread polls for handling the networking and activity task will be created.
    # @param block the activity logic
    def start(&block)
      @sink = SizedQueue.new(@queue_max)
      @activities = Set.new
      start_heartbeat_worker(@activities)
      start_workers(@activities, block, @sink)
      start_pollers(@activities, @sink)
      wait
    end

    # Returns the current task queue size.
    # @return [Integer] the current task queue size
    def queue_size
      return 0 if @sink.nil?
      @sink.size
    end

    # Returns the current count of activates. This count will be sum of all activities being executed and count of
    # activities that had been polled and added to internal queue waiting for activity thread to process them.
    # @return [Integer] the current activity
    def activities_count
      return 0 if @activities.nil?
      @activities.size
    end

    private

    def start_pollers(activities, sink)
      @pollers = Array.new(@pollers_count) do
        PollerWorker.new(
          states: @states,
          activity_arn: @activity_arn,
          sink: sink,
          activities: activities,
          max_retry: @max_retry
        )
      end
      @pollers.each(&:start)
    end

    def start_workers(activities, block, sink)
      @workers = Array.new(@workers_count) do
        ActivityWorker.new(
          states: @states,
          block: block,
          sink: sink,
          activities: activities,
          max_retry: @max_retry
        )
      end
      @workers.each(&:start)
    end

    def start_heartbeat_worker(activities)
      @heartbeat_worker = HeartbeatWorker.new(
        states: @states,
        activities: activities,
        heartbeat_delay: @heartbeat_delay,
        max_retry: @max_retry
      )
      @heartbeat_worker.start
    end

    def wait
      sleep
    rescue Interrupt
      shutdown
    ensure
      Thread.current.exit
    end

    def shutdown
      stop_workers(@pollers)
      wait_workers(@pollers)
      wait_activities_drained
      stop_workers(@workers)
      wait_activities_completed
      shutdown_workers(@workers)
      shutdown_worker(@heartbeat_worker)
    end

    def shutdown_workers(workers)
      workers.each do |worker|
        shutdown_worker(worker)
      end
    end

    def shutdown_worker(worker)
      worker.kill
    end

    def wait_workers(workers)
      workers.each(&:wait)
    end

    def wait_activities_drained
      wait_condition { @sink.empty? }
    end

    def wait_activities_completed
      wait_condition { @activities.empty? }
    end

    def wait_condition(&block)
      loop do
        break if block.call
        sleep(1)
      end
    end

    def stop_workers(workers)
      workers.each(&:stop)
    end

    class Worker
      def initialize
        @logger = Logger.new(STDOUT)
        @running = false
        @thread = nil
      end

      def run
        raise 'Method run hasn\'t been implemented'
      end

      def process
        loop do
          begin
            break unless @running
            run
          rescue => e
            puts e
            @logger.error('Unexpected error had occurred')
            @logger.error(e)
          end
        end
      end

      def start
        return unless @thread.nil?
        @running = true
        @thread = Thread.new do
          process
        end
      end

      def stop
        @running = false
      end

      def kill
        return if @thread.nil?
        @thread.kill
        @thread = nil
      end

      def wait
        @thread.join
      end
    end

    class PollerWorker < Worker
      def initialize(options = {})
        @states = options[:states]
        @activity_arn = options[:activity_arn]
        @sink = options[:sink]
        @activities = options[:activities]
        @max_retry = options[:max_retry]
        @logger = Logger.new(STDOUT)
      end

      def run
        activity_task = StepFunctions.with_retries(max_retry: @max_retry) do
          begin
            @states.get_activity_task(activity_arn: @activity_arn)
          rescue => e
            @logger.error('Failed to retrieve activity task')
            @logger.error(e)
          end
        end
        return if activity_task.nil? || activity_task.task_token.nil?
        @activities.add(activity_task.task_token)
        @sink.push(activity_task)
      end
    end

    class ActivityWorker < Worker
      def initialize(options = {})
        @states = options[:states]
        @block = options[:block]
        @sink = options[:sink]
        @activities = options[:activities]
        @max_retry = options[:max_retry]
        @logger = Logger.new(STDOUT)
      end

      def run
        activity_task = @sink.pop
        result = @block.call(JSON.parse(activity_task.input))
        send_task_success(activity_task, result)
      rescue => e
        send_task_failure(activity_task, e)
      ensure
        @activities.delete(activity_task.task_token) unless activity_task.nil?
      end

      def send_task_success(activity_task, result)
        StepFunctions.with_retries(max_retry: @max_retry) do
          begin
            @states.send_task_success(
              task_token: activity_task.task_token,
              output: JSON.dump(result)
            )
          rescue => e
            @logger.error('Failed to send task success')
            @logger.error(e)
          end
        end
      end

      def send_task_failure(activity_task, error)
        StepFunctions.with_retries do
          begin
            @states.send_task_failure(
              task_token: activity_task.task_token,
              cause: error.message
            )
          rescue => e
            @logger.error('Failed to send task failure')
            @logger.error(e)
          end
        end
      end
    end

    class HeartbeatWorker < Worker
      def initialize(options = {})
        @states = options[:states]
        @activities = options[:activities]
        @heartbeat_delay = options[:heartbeat_delay]
        @max_retry = options[:max_retry]
        @logger = Logger.new(STDOUT)
      end

      def run
        sleep(@heartbeat_delay)
        @activities.each do |token|
          send_heartbeat(token)
        end
      end

      def send_heartbeat(token)
        StepFunctions.with_retries(max_retry: @max_retry) do
          begin
            @states.send_task_heartbeat(token)
          rescue => e
            @logger.error('Failed to send heartbeat for activity')
            @logger.error(e)
          end
        end
      rescue => e
        @logger.error('Failed to send heartbeat for activity')
        @logger.error(e)
      end
    end
  end
end
