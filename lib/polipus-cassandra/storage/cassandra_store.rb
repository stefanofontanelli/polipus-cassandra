# encoding: UTF-8
require 'cassandra'
require 'multi_json'
require 'polipus'
require 'thread'
require 'zlib'

module Polipus
  module Storage
    class CassandraStore < Base
      # some doc here

      attr_accessor :cluster, :keyspace, :table

      BINARY_FIELDS = %w(body headers user_data)

      def initialize(options = {})
        @cluster = options[:cluster]
        @keyspace = options[:keyspace]
        @table = options[:table]
        @except = options[:except] || []
        @semaphore = Mutex.new
      end

      # {
      #   'url'           => @url.to_s,
      #   'headers'       => Marshal.dump(@headers),
      #   'body'          => @body,
      #   'links'         => links.map(&:to_s),
      #   'code'          => @code,
      #   'depth'         => @depth,
      #   'referer'       => @referer.to_s,
      #   'redirect_to'   => @redirect_to.to_s,
      #   'response_time' => @response_time,
      #   'fetched'       => @fetched,
      #   'user_data'     => @user_data.nil? ? {} : @user_data.marshal_dump,
      #   'fetched_at'    => @fetched_at,
      #   'error'         => @error.to_s
      # }

      def add(page)
        @semaphore.synchronize do
          table_ = [keyspace, table].compact.join '.'
          uuid_ = uuid(page)
          obj = page.to_hash
          Array(@except).each { |e| obj.delete e.to_s }
          json = MultiJson.encode(obj)
          statement = "INSERT INTO #{table_} (uuid, page) VALUES (?, ?);"
          session.execute(session.prepare(statement),
                          arguments: [uuid_, Zlib::Deflate.deflate(json)])
          uuid_
        end
      end

      def clear
        table_ = [keyspace, table].compact.join '.'
        statement = "DROP TABLE #{table_};"
        session.execute statement
      end

      def count
        fail('Count is not supported in Cassandra.')
      end

      def each
        table_ = [keyspace, table].compact.join '.'
        statement = "SELECT * FROM #{table_};"
        session.execute(statement).each do |data|
          page = load_page(data) unless data.nil?
          yield data['uuid'], page
        end
      end

      def exists?(page)
        @semaphore.synchronize do
          table_ = [keyspace, table].compact.join '.'
          statement = "SELECT uuid FROM #{table_} WHERE uuid = ? LIMIT 1;"
          results = session.execute(session.prepare(statement),
                                    arguments: [uuid(page)])
          !results.first.nil?
        end
      end

      def get(page)
        @semaphore.synchronize do
          table_ = [keyspace, table].compact.join '.'
          statement = "SELECT * FROM #{table_} WHERE uuid = ? LIMIT 1;"
          results = session.execute(session.prepare(statement),
                                    arguments: [uuid(page)])
          data = results.first
          load_page(data) unless data.nil?
        end
      end

      def keyspace!(replication = nil, durable_writes = true)
        replication ||= "{'class': 'SimpleStrategy', 'replication_factor': '1'}"
        statement = "CREATE KEYSPACE IF NOT EXISTS #{keyspace} WITH replication = #{replication} AND durable_writes = #{durable_writes};"
        cluster.connect.execute statement
      end

      def remove(page)
        @semaphore.synchronize do
          table_ = [keyspace, table].compact.join '.'
          statement = "DELETE FROM #{table_} WHERE uuid = ?;"
          session.execute(session.prepare(statement),
                          arguments: [uuid(page)])
          true
        end
      end

      def session
        @session = @cluster.connect(keyspace)
      end

      def table!(properties = nil)
        table_ = [keyspace, table].compact.join '.'
        def_ = "CREATE TABLE IF NOT EXISTS #{table_} (uuid TEXT PRIMARY KEY, page BLOB)"
        props = properties.to_a.join(' AND ')
        statement = props.empty? ? "#{def_};" : "#{def_} WITH #{props};"
        session.execute statement
      end

      def load_page(data)
        json = Zlib::Inflate.inflate(data['page'])
        hash = MultiJson.decode(json)
        page = Page.from_hash(hash)
        page.fetched_at = 0 if page.fetched_at.nil?
        page
      end
    end
  end
end
