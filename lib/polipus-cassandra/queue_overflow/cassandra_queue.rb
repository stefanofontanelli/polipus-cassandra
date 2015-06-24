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
      # Taking some data from our backend.production.mongodb.gild.com/polipus
      # I found:
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

      def initialize(options = {})
        @cluster = options[:cluster]
        @keyspace = options[:keyspace]
        @table = options[:table]
        @semaphore = Mutex.new
        @options = options
        @logger = @options[:logger] ||= Logger.new(STDOUT).tap { |l| l.level = Logger::INFO }
        # @options[:ensure_uniq] ||= false
        # @options[:ensure_uniq] && ensure_index
      end

      # Length aka Size aka Count is not supported in Cassandra... this is not
      # your POSQL.
      def length
        fail('Count is not supported in Cassandra.')
      end

      # Return true if the table has no rows.
      # This is achieved with a 'LIMIT 1' query.
      def empty?
        @semaphore.synchronize do
          table_ = [keyspace, table].compact.join '.'
          statement = "SELECT uuid FROM #{table_} LIMIT 1;"
          results = session.execute(session.prepare(statement), arguments: [])
          !results.first.nil?
        end
      end

      # Clear is a fancy name for a DROP TABLE IF EXISTS table_.
      def clear
        table_ = [keyspace, table].compact.join '.'
        statement = "DROP TABLE IF EXISTS #{table_};"
        session.execute statement
      end

      def push(data)
        @semaphore.synchronize do

          obj = MultiJson.decode(data)
          raise 'Data received does not have URL' unless obj.has_key?('url')

          table_ = [keyspace, table].compact.join('.')
          uuid_ = uuid(data['url'])

          begin
            url = obj.fetch('url', nil)
            depth = obj.fetch('depth', nil)
            fetched = obj.fetch('fetched', nil)
            if obj.has_key?('payload') && !obj['payload'].empty?
              payload = MultiJson.encode(obj['payload'])
            else
              payload = nil
            end

            column_names = %w[ uuid url depth fetched payload ]
            values_placeholders = column_names.map{|_| '?'}.join(',')
            statement = "INSERT INTO #{table_} ( #{column_names.join(',')} ) VALUES (#{values_placeholders});"

            session.execute(
              session.prepare(statement),
              arguments: [
                uuid_,
                url,
                depth,
                fetched,
                payload
              ])
          rescue Encoding::UndefinedConversionError
            puts $!.error_char.dump
            puts $!.error_char.encoding
          end

          uuid_
        end
      end

      def pop(_ = false)
        # This method will do the following:
        # - find
        # - sort
        # - delete
        # - return
        fail "Not implemented yet!"
      end

      alias_method :size,  :length
      alias_method :dec,   :pop
      alias_method :shift, :pop
      alias_method :enc,   :push
      alias_method :<<,    :push

      def uuid(data)
        if @include_query_string_in_uuid.nil?
          @include_query_string_in_uuid = true
        end
        url_to_hash = @include_query_string_in_uuid ? data['url'].to_s : data['url'].to_s.gsub(/\?.*$/, '')
        Digest::MD5.hexdigest url_to_hash.gsub('https://', 'http://')
      end

      def keyspace!(replication = nil, durable_writes = true)
        replication ||= "{'class': 'SimpleStrategy', 'replication_factor': '1'}"
        statement = "CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH replication = #{replication} AND durable_writes = #{durable_writes};"
        cluster.connect.execute statement
      end

      def session
        @session = @cluster.connect(keyspace)
      end

      # Taking a look in the Cassandra KEYSPACE you will found:
      #
      # cqlsh> DESCRIBE KEYSPACE polipus_queue_overflow_linkedin
      #
      # CREATE KEYSPACE polipus_queue_overflow_linkedin WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
      #
      # CREATE TABLE polipus_queue_overflow_linkedin.linkedin_overflow (
      #     uuid text PRIMARY KEY,
      #     depth int,
      #     fetched boolean,
      #     payload text,
      #     url text
      # ) WITH bloom_filter_fp_chance = 0.01
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
      # With sample data:
      #
      # cqlsh> select * from polipus_queue_overflow_linkedin.linkedin_overflow ;
      #
      #  uuid                             | depth | fetched | payload | url
      # ----------------------------------+-------+---------+---------+-------------------------------------------------------------------------
      #  572d4e421e5e6b9bc11d815e8a027112 |     3 |   False |    null | https://www.linkedin.com/pub/jennifer-peterson/13/614/69a?trk=pub-pbmap
      #
      # (1 rows)
      def table!(properties = nil)
        table_ = [keyspace, table].compact.join '.'
        def_ = "CREATE TABLE IF NOT EXISTS #{table_}
          (
            uuid TEXT PRIMARY KEY,
            url TEXT,
            payload TEXT,
            depth INT,
            fetched BOOLEAN
          )"
        props = properties.to_a.join(' AND ')
        statement = props.empty? ? "#{def_};" : "#{def_} WITH #{props};"
        session.execute statement
      end
    end
  end
end
