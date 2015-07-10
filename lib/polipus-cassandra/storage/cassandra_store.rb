# encoding: UTF-8
require 'cassandra'
require 'multi_json'
require 'polipus'
require 'thread'
require 'zlib'

module Polipus
  module Storage
    class CassandraStore < Base

      # CassandraStore wants to persists documents (please ignore the jargon
      # inherited from MongoDB) like the following JSON-ish entry:
      #
      # > db['linkedin-refresh'].find({})
      #
      #   {
      #     "_id" : ObjectId("...."),
      #     "url" : "https://www.awesome.org/meh",
      #     "code" : 200,
      #     "depth" : 0,
      #     "referer" : "",
      #     "redirect_to" : "",
      #     "response_time" : 1313,
      #     "fetched" : true,
      #     "user_data" :
      #       {
      #         "imported" : false,
      #         "is_developer" : false,
      #         "last_modified" : null
      #       },
      #      "fetched_at" : 1434977757,
      #      "error" : "",
      #      "uuid" : "4ddce293532ea2454356a4210e61c363"
      #  }

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
          Array(@except).each { |e| obj.delete(e.to_s) }

          begin
            BINARY_FIELDS.each do |field|
              obj[field] = obj[field].to_s.encode('UTF-8', {
                invalid: :replace,
                undef: :replace,
                replace: '?' }) if can_be_converted?(obj[field])
              # ec = Encoding::Converter.new("ASCII-8BIT", "UTF-8")
              # obj[field] = ec.convert(obj[field]) if can_be_converted?(obj[field])
              # obj[field] = obj[field].force_encoding('ASCII-8BIT').force_encoding('UTF-8') if can_be_converted?(obj[field])
            end

            json = MultiJson.encode(obj)

            url = obj.fetch('url', nil)
            code = obj.fetch('code', nil)
            depth = obj.fetch('depth', nil)
            referer = obj.fetch('referer', nil)
            redirectto = obj.fetch('redirect_to', nil)
            response_time = obj.fetch('response_time', nil)
            fetched = obj.fetch('fetched', nil)
            error = obj.fetch('error', nil)
            page = Zlib::Deflate.deflate(json)

            if obj.has_key?('user_data') && !obj['user_data'].empty?
              user_data = MultiJson.encode(obj['user_data'])
            else
              user_data = nil
            end

            value = obj.fetch('fetched_at', nil)
            fetched_at = case value
            when Fixnum
              Time.at(value)
            when String
              Time.parse(value)
            else
              nil
            end

            column_names = %w[ uuid url code depth referer redirect_to response_time fetched user_data fetched_at error page ]
            values_placeholders = column_names.map{|_| '?'}.join(',')
            statement = "INSERT INTO #{table_} ( #{column_names.join(',')} ) VALUES (#{values_placeholders});"

            session.execute(
              session.prepare(statement),
              arguments: [
                uuid_,
                url,
                code,
                depth,
                referer,
                redirectto,
                response_time,
                fetched,
                user_data,
                fetched_at,
                error,
                page
              ])

          rescue Encoding::UndefinedConversionError
            puts $!.error_char.dump
            puts $!.error_char.encoding
          end

          uuid_
        end
      end

      def clear
        table_ = [keyspace, table].compact.join '.'
        statement = "DROP TABLE #{table_};"
        session.execute statement
      end

      # TBH I'm not sure if being "defensive" and returning 0/nil in case
      # the results is_empty? ... I'm leaving (now) the code simple and noisy
      # if something went wrong in the COUNT.
      def count
        table_ = [keyspace, table].compact.join '.'
        statement = "SELECT COUNT (*) FROM #{table_} ;"
        result = session.execute(statement)
        result.first['count']
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
        replication ||= "{'class': 'SimpleStrategy', 'replication_factor': '3'}"
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
        @session ||= @cluster.connect(keyspace)
      end

      def table!(properties = nil)
        table_ = [keyspace, table].compact.join '.'
        def_ = "CREATE TABLE IF NOT EXISTS #{table_}
          (
            uuid TEXT PRIMARY KEY,
            url TEXT,
            code INT,
            depth INT,
            referer TEXT,
            redirect_to TEXT,
            response_time BIGINT,
            fetched BOOLEAN,
            user_data TEXT,
            fetched_at TIMESTAMP,
            error TEXT,
            page BLOB
          )"
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

      private

      def can_be_converted?(field)
        !field.nil? && field.is_a?(String) && !field.empty?
      end
    end
  end
end
