# encoding: UTF-8
require 'cassandra'
require 'polipus'

module Polipus
  module QueueOverflow
    class CassandraQueue

      # Contains the `attempts_wrapper` implementation.
      include Enhancements

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
      # Taking some data from our backend.production.mongodb.gild.com/polipus
      # I found:
      #
      # db.polipus_q_overflow_linkedin_refresh_queue_overflow.find().limit(1)
      # {
      #   "_id" : ObjectId("544072d0e3d55b0db700021c"),
      #   "payload" : "{\"url\":\"http://www.linkedin.com/pub/joseph-chanlatte/6/116/374\",\"depth\":0,\"fetched\":false}"
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

      # Length aka Size aka Count is not supported in Cassandra... this is not
      # your POSQL.
      def length
        fail('Count is not supported in Cassandra.')
      end

      # Return true if the table has no rows.
      # This is achieved with a 'SELECT WITH LIMIT 1' query.
      def empty?
        return get.first.nil?
      end

      # Clear is a fancy name for a DROP TABLE IF EXISTS <table_>.
      def clear
        attempts_wrapper do
          table_ = [keyspace, table].compact.join '.'
          statement = "DROP TABLE IF EXISTS #{table_} ;"
          session.execute(statement)
        end
      end

      # push is your the "write into Cassandra" method.
      def push(data)
        return nil if data.nil?
        attempts_wrapper do
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
      end

      def pop(_ = false)
        # A recap: pop should remove oldest N messages and return to the caller.
        # This method will do the following:
        # - find
        # - sort
        # - delete
        # - return
        fail "Not implemented yet!"
        attempts_wrapper do
        end
      end

      alias_method :size, :length
      alias_method :dec, :pop
      alias_method :shift, :pop
      alias_method :enc, :push
      alias_method :<<, :push

      def keyspace!(replication = nil, durable_writes = true)
        replication ||= "{'class': 'SimpleStrategy', 'replication_factor': '1'}"
        statement = "CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH replication = #{replication} AND durable_writes = #{durable_writes};"
        attempts_wrapper { cluster.connect.execute(statement) }
      end

      def session
        if @session.nil?
          attempts_wrapper { @session = @cluster.connect(keyspace) }
        end
        @session
      end

      # Taking a look in the Cassandra KEYSPACE you will found:
      #
      # cqlsh> DESCRIBE KEYSPACE polipus_queue_overflow_linkedin ;
      #
      # CREATE KEYSPACE polipus_queue_overflow_linkedin WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
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
      # ---------------------------------+--------------------------------------+---------
      #  polipus_queue_overflow_linkedin | 67513750-1bef-11e5-ba03-45190569f1f0 |    null
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
        attempts_wrapper { session.execute(statement) }
      end

      private

      def options_are_valid?(options)
        options.has_key?(:cluster) && options.has_key?(:keyspace) && options.has_key?(:table)
      end

      def limit_is_valid?(limit)
        !limit.nil? && limit.respond_to?(:to_i) && limit.to_i > 0
      end

      def get(limit = 1)
        raise ArgumentError("Invalid limit value: must be an INTEGER greater than 1.") unless limit_is_valid?(limit)
        attempts_wrapper do
          table_ = [keyspace, table].compact.join '.'
          statement = "SELECT queue_name, created_at, payload FROM #{table_} LIMIT #{limit.to_i} ;"
          @semaphore.synchronize do
            return session.execute(session.prepare(statement), arguments: [])
          end
        end
      end
    end
  end
end
