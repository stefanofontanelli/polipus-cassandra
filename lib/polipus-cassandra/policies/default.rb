# encoding: utf-8
require 'cassandra'

module Corm
  module Retry
    module Policies
      class Default
        include Cassandra::Retry::Policy

        def read_timeout(_statement, consistency, _required, _received, retrieved, retries)
          return reraise if retries >= 5
          sleep(retries.to_f + Random.rand(0.0..1.0))
          retrieved ? reraise : try_again(consistency)
        end

        def write_timeout(_statement, consistency, _type, _required, _received, retries)
          return reraise if retries >= 5
          sleep(retries.to_f + Random.rand(0.0..1.0))
          try_again(consistency)
        end

        def unavailable(_statement, consistency, _required, _alive, retries)
          return reraise if retries >= 5
          sleep(retries.to_f + Random.rand(0.0..1.0))
          try_again(consistency)
        end
      end
    end
  end
end
