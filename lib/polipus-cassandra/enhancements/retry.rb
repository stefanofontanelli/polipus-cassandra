# encoding: utf-8

module Polipus
  module QueueOverflow
    module Enhancements

      def enhancements_logger(logger = nil)
        Logger.new(STDOUT).tap { |l| l.level = Logger::INFO } unless logger
      end

      RESCUED_CASSANDRA_EXCEPTIONS = [
        ::Cassandra::Errors::ExecutionError,
        ::Cassandra::Errors::IOError,
        ::Cassandra::Errors::InternalError,
        ::Cassandra::Errors::NoHostsAvailable,
        ::Cassandra::Errors::ServerError,
        ::Cassandra::Errors::TimeoutError
      ]

      # Trying to rescue from a Cassandra::Error
      #
      # The relevant documentation is here (version 2.1.3):
      # https://datastax.github.io/ruby-driver/api/error/
      #
      # Saving from:
      #
      # - ::Cassandra::Errors::ExecutionError
      # - ::Cassandra::Errors::IOError
      # - ::Cassandra::Errors::InternalError
      # - ::Cassandra::Errors::NoHostsAvailable
      # - ::Cassandra::Errors::ServerError
      # - ::Cassandra::Errors::TimeoutError
      #
      # Ignoring:
      # - Errors::ClientError
      # - Errors::DecodingError
      # - Errors::EncodingError
      # - Errors::ValidationError
      #
      # A possible and maybe-good refactoring could be refine for the
      # network related issues.
      def attempts_wrapper(attempts = 3, &block)
        (1..attempts).each do |i|
          begin
            return block.call() if block_given?
          rescue *RESCUED_CASSANDRA_EXCEPTIONS => e
            sleep_for = i * Integer((rand + 1.5) * 100) / Float(100)
            self.enhancements_logger.error { "(#{i}/#{attempts} attempts) Bad fail! Retry in #{sleep_for} seconds to recover  #{e.class.name}: #{e.message}" }
            sleep(sleep_for)
          end
        end
        nil
      end

      module_function(:attempts_wrapper)
      public :attempts_wrapper
    end
  end
end
