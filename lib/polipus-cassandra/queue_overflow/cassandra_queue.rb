# encoding: UTF-8
require 'cassandra'
require 'polipus'

module Polipus
  module QueueOverflow
    class CassandraQueue

      # CassandraQueue wants to persists documents (please, still ignore the
      # jargon inherited from Mongo) like the following JSON-ish entry.
      #
      # There is no superclass here but I've in mind the interface implicitly
      # defined by Polipus::QueueOverflow::DevNullQueue that, more or less has:
      #
      # def initialize
      # def length
      # def empty?
      # def clear
      # def push(_data)
      # def pop(_ = false)
      #
      # Taking some data from our backend.production.*****.com/polipus
      # I found:
      #
      # mongos> db.getCollectionNames()
      # [
      #         "data-com-companies",
      #         "data_com_companies",
      #         "googleplus",
      #         "linkedin",
      #         "linkedin-companies",
      #         "linkedin_companies_parsed",
      #         "linkedin_jobs",
      #         "linkedin_jobs_parsed",
      #         "linkedin_pages_errors",
      #         "polipus_q_overflow_data-com-companies_queue_overflow",
      #         "polipus_q_overflow_data_com_companies_queue_overflow",
      #         "polipus_q_overflow_googleplus_queue_overflow",
      #         "polipus_q_overflow_linkedin-companies_queue_overflow",
      #         "polipus_q_overflow_linkedin_jobs_queue_overflow",
      #         "polipus_q_overflow_linkedin_jobs_queue_overflow_old",
      #         "polipus_q_overflow_linkedin_refresh_queue_overflow",
      #         "system.indexes"
      # ]
      #
      # mongos> db.getCollection("polipus_q_overflow_linkedin_jobs_queue_overflow").find().limit(1)
      # {
      #   "_id" : ObjectId("54506b98e3d55b20c40b32d3"),
      #   "payload" : "{\"url\":\"https://www.linkedin.com/job/product-designer-jobs/?page_num=7&trk=jserp_pagination_next\",\"depth\":6,\"referer\":\"https://www.linkedin.com/job/product-designer-jobs/?page_num=6&trk=jserp_pagination_6\",\"fetched\":false}"
      # }
      #
      # mongos> db.polipus_q_overflow_linkedin_refresh_queue_overflow.find().limit(10)
      # {
      #   "_id" : ObjectId("544072b6e3d55b0db7000001"),
      #   "payload" : "{\"url\":\"http://www.linkedin.com/in/*****\",\"depth\":0,\"fetched\":false}"
      # }
      #
      # We also assume this MonkeyPatch:
      # Polipus::QueueOverflow.cassandra_queue(namespace, options = {})
      # that returns instances of this class.

      attr_accessor :cluster, :keyspace, :table

      # There is a validation enforced to `:keyspace` and `:table` because
      # Cassandra is not happy when a keyspace or a table name contains an
      # hyphen.
      def initialize(options = {})
        raise ArgumentError unless options_are_valid?(options)
        @cluster = options[:cluster]
        @keyspace = options[:keyspace].gsub("-", "_")
        @table = options[:table].gsub("-", "_")
        @semaphore = Mutex.new
        @options = options
        @timeuuid_generator = Cassandra::Uuid::Generator.new
        @logger = @options[:logger] ||= Logger.new(STDOUT).tap { |l| l.level = Logger::INFO }
      end

      # Length aka Size aka Count is supported in Cassandra... like your POSQL
      # you can COUNT.
      #
      # SELECT COUNT (*) FROM keyspace.table_name;
      #
      # TBH I'm not sure if being "defensive" and returning 0/nil in case
      # the results is_empty? ... I'm leaving (now) the code simple and noisy
      # if something went wrong in the COUNT.
      def length
        table_ = [keyspace, table].compact.join '.'
        statement = "SELECT COUNT (*) FROM #{table_} ;"
        result = session.execute(statement)
        result.first['count']
      end

      # Return true if the table has no rows.
      # This is achieved with a 'SELECT WITH LIMIT 1' query.
      def empty?
        return get.first.nil?
      end

      # Clear is a fancy name for a DROP TABLE IF EXISTS <table_>.
      def clear
        table_ = [keyspace, table].compact.join '.'
        statement = "DROP TABLE IF EXISTS #{table_} ;"
        session.execute(statement)
      end

      # push is your the "write into Cassandra" method.
      def push(data)
        return nil if data.nil?
        obj = MultiJson.decode(data)

        table_ = [keyspace, table].compact.join('.')
        queue_name = @keyspace
        created_at = @timeuuid_generator.now

        begin
          @semaphore.synchronize do

            if obj.has_key?('payload') && !obj['payload'].empty?
              payload = MultiJson.encode(obj['payload'])
            else
              payload = nil
            end

            column_names = %w[ queue_name created_at payload ]
            values_placeholders = column_names.map{|_| '?'}.join(',')
            statement = "INSERT INTO #{table_} ( #{column_names.join(',')} ) VALUES (#{values_placeholders});"

            session.execute(
              session.prepare(statement),
              arguments: [
                queue_name,
                created_at,
                payload
              ])
          end
        rescue Encoding::UndefinedConversionError
          puts $!.error_char.dump
          puts $!.error_char.encoding
        end

        @logger.debug { "Writing this entry [#{[queue_name, created_at].to_s}]" }
        [queue_name, created_at].to_s
      end

      # Pop removes 'n' entries from the overflow table (treated as a queue)
      # and returns a paged result.
      # results.class #=> Cassandra::Results::Paged
      #
      # Polipus is expecting a String, that will be JSONparsed with the purpose
      # to build a
      def pop(n = 1)
        # A recap: pop should remove oldest N messages and return to the caller.
        #
        # Let's see how this queue is implemented.
        # In redis, messages are LPUSH-ed:
        #
        #  4 - 3 - 2 - 1 --> REDIS
        #      4 - 3 - 2 --> REDIS
        #          4 - 3 --> REDIS
        #              4 --> REDIS
        #
        # Then, in the fast_dequeue, are RPOP-ped:
        #
        # REDIS --> 1
        # REDIS --> 2 - 1
        # REDIS --> 3 - 2 - 1
        # REDIS --> 4 - 3 - 2 - 1
        #
        # Then, are received in this order:
        # [1] -> TimeUUID(1) = ...
        # [2] -> TimeUUID(1) = ...
        # [3] -> TimeUUID(1) = ...
        # [4] -> TimeUUID(1) = ...
        #
        # As you can see below, are ORDER BY (created_at ASC)... that means
        # "olders first". When using 'LIMIT n' in a query, you get the 'n'
        # olders entries.
        #
        # cqlsh> SELECT  * FROM  polipus_queue_overflow_linkedin.linkedin_overflow ;
        #
        #  queue_name                      | created_at                           | payload
        # ---------------------------------+--------------------------------------+---------
        #  polipus_queue_overflow_linkedin | 4632d49c-1c04-11e5-844b-0b314c777502 |     "1"
        #  polipus_queue_overflow_linkedin | 46339f8a-1c04-11e5-844b-0b314c777502 |     "2"
        #  polipus_queue_overflow_linkedin | 46349962-1c04-11e5-844b-0b314c777502 |     "3"
        #  polipus_queue_overflow_linkedin | 46351860-1c04-11e5-844b-0b314c777502 |     "4"
        #
        # (4 rows)
        # cqlsh> SELECT  * FROM  polipus_queue_overflow_linkedin.linkedin_overflow LIMIT 1;
        #
        #  queue_name                      | created_at                           | payload
        # ---------------------------------+--------------------------------------+---------
        #  polipus_queue_overflow_linkedin | 4632d49c-1c04-11e5-844b-0b314c777502 |     "1"
        #
        # (1 rows)
        #
        table_ = [keyspace, table].compact.join '.'
        results = get(n)
        results.each do |entry|
          statement = "DELETE FROM #{table_} WHERE queue_name = '#{entry['queue_name']}' AND created_at = #{entry['created_at']} ;"
          session.execute(statement)
        end

        # Let's rispect the API as expected by Polipus.
        # Otherwise the execute returns a Cassandra::Results::Paged
        if !results.nil? && results.respond_to?(:count) && results.count == 1
          return results.first['payload']
        end
        return results
      end

      alias_method :size, :length
      alias_method :dec, :pop
      alias_method :shift, :pop
      alias_method :enc, :push
      alias_method :<<, :push

      def keyspace!(replication = nil, durable_writes = true)
        replication ||= "{'class': 'SimpleStrategy', 'replication_factor': '3'}"
        statement = "CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH replication = #{replication} AND durable_writes = #{durable_writes};"
        cluster.connect.execute(statement)
      end

      def session
        @session ||= @cluster.connect(keyspace)
      end

      # Taking a look in the Cassandra KEYSPACE you will found:
      #
      # cqlsh> DESCRIBE KEYSPACE polipus_queue_overflow_linkedin ;
      #
      # CREATE KEYSPACE polipus_queue_overflow_linkedin WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;
      #
      # CREATE TABLE polipus_queue_overflow_linkedin.linkedin_overflow (
      #     queue_name text,
      #     created_at timeuuid,
      #     payload text,
      #     PRIMARY KEY (queue_name, created_at)
      # ) WITH CLUSTERING ORDER BY (created_at ASC)
      #     AND bloom_filter_fp_chance = 0.01
      #     AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
      #     AND comment = ''
      #     AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
      #     AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
      #     AND dclocal_read_repair_chance = 0.1
      #     AND default_time_to_live = 0
      #     AND gc_grace_seconds = 864000
      #     AND max_index_interval = 2048
      #     AND memtable_flush_period_in_ms = 0
      #     AND min_index_interval = 128
      #     AND read_repair_chance = 0.0
      #     AND speculative_retry = '99.0PERCENTILE';
      #
      # This means that:
      # - queue_name is partition key;
      # - created_at is clustering key;
      #
      # With sample data:
      #
      # cqlsh> SELECT  * FROM  polipus_queue_overflow_linkedin.linkedin_overflow LIMIT 1 ;
      #
      #  queue_name                      | created_at                           | payload
      # ---------------------------------+--------------------------------------+---------------------------------------------------------------------------------+
      #  polipus_queue_overflow_linkedin | de17ece6-1e5e-11e5-b997-47a87c40c422 | "{\"url\":\"http://www.linkedin.com/in/foobar\",\"depth\":0,\"fetched\":false}"
      #
      # (1 rows)
      # cqlsh>
      #
      def table!(properties = nil)
        table_ = [keyspace, table].compact.join '.'
        def_ = "CREATE TABLE IF NOT EXISTS #{table_}
          (
            queue_name TEXT,
            created_at TIMEUUID,
            payload TEXT,
            PRIMARY KEY (queue_name, created_at)
          )"
        props = Array(properties).join(' AND ')
        statement = props.empty? ? "#{def_};" : "#{def_} WITH #{props};"
        session.execute(statement)
      end

      private

      def options_are_valid?(options)
        options.has_key?(:cluster) && options.has_key?(:keyspace) && options.has_key?(:table)
      end

      def limit_is_valid?(limit)
        !limit.nil? && limit.respond_to?(:to_i) && limit.to_i > 0
      end

      # results.class => Cassandra::Results::Paged
      def get(limit = 1)
        # coerce to int if a TrueClass/FalseClass is given.
        limit = 1 if [true, false].include?(limit)

        raise ArgumentError.new("Invalid limit value: must be an INTEGER greater than 1 (got #{limit.inspect}).") unless limit_is_valid?(limit)
        table_ = [keyspace, table].compact.join '.'
        statement = "SELECT queue_name, created_at, payload FROM #{table_} LIMIT #{limit.to_i} ;"
        @semaphore.synchronize do
          return session.execute(session.prepare(statement), arguments: [])
        end
      end
    end
  end
end
