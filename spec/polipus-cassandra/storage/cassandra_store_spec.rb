# encoding: UTF-8
require 'cassandra'
require 'logger'
require 'polipus-cassandra'
require 'spec_helper'

describe Polipus::Storage::CassandraStore do
  before(:all)do
    @logger = Logger.new(STDOUT).tap { |logger| logger.level = Logger::WARN }
    @cluster = Cassandra.cluster hosts: ['127.0.0.1'], logger: @logger
    @keyspace = 'polipus_cassandra_test'
    @table = 'cassandra_store_test'
    @storage = Polipus::Storage::CassandraStore.new(
      cluster: @cluster,
      keyspace: @keyspace,
      table: @table,
    )

    @storage.keyspace!
    @storage.table!
  end

  after(:all) do
    @storage.clear
  end

  it 'should store a page' do
    p = page_factory 'http://www.google.com'
    uuid = @storage.add p
    expect(uuid).to eq('ed646a3334ca891fd3467db131372140')
    p = @storage.get p
    expect(p.url.to_s).to eq('http://www.google.com')
    expect(p.body).to eq('<html></html>')
  end

  it 'should update a page' do
    p = page_factory 'http://www.google.com', code: 301
    @storage.add p
    p = @storage.get p
    expect(p.code).to eq(301)
  end

  it 'should iterate over stored pages' do
    @storage.each do |k, page|
      expect(k).to eq('ed646a3334ca891fd3467db131372140')
      expect(page.url.to_s).to eq('http://www.google.com')
    end
  end

  it 'should delete a page' do
    p = page_factory 'http://www.google.com', code: 301
    @storage.remove p
    expect(@storage.get(p)).to be_nil
  end

  it 'should store a page removing a query string from the uuid generation' do
    p = page_factory 'http://www.asd.com/?asd=lol'
    p_no_query = page_factory 'http://www.asd.com/?asdas=dasda&adsda=1'
    @storage.include_query_string_in_uuid = false
    @storage.add p
    expect(@storage.exists?(p_no_query)).to be_truthy
    @storage.remove p
  end

  it 'should store a page removing a query string from the uuid generation no ending slash' do
    p = page_factory 'http://www.asd.com?asd=lol'
    p_no_query = page_factory 'http://www.asd.com'
    @storage.include_query_string_in_uuid = false
    @storage.add p
    expect(@storage.exists?(p_no_query)).to be_truthy
    @storage.remove p
  end

  it 'should store a page with user data associated' do
    p = page_factory 'http://www.user.com'
    p.user_data.name = 'Test User Data'
    @storage.add p
    expect(@storage.exists?(p)).to be_truthy
    p = @storage.get(p)
    expect(p.user_data.name).to eq('Test User Data')
    @storage.remove p
  end

  it 'should honor the except parameters' do
    p = page_factory 'http://www.user-doo.com',  code: 200, body: '<html></html>'
    @storage.add p
    p = @storage.get p
    expect(p.body).not_to be_empty
    @storage.remove p
  end

  it 'should return false if a doc not exists' do
    @storage.include_query_string_in_uuid = false
    p_other  = page_factory 'http://www.asdrrrr.com'
    expect(@storage.exists?(p_other)).to be_falsey
    @storage.add p_other
    expect(@storage.exists?(p_other)).to be_truthy
    p_other  = page_factory 'http://www.asdrrrr.com?trk=asd-lol'
    expect(@storage.exists?(p_other)).to be_truthy
    @storage.include_query_string_in_uuid = true
    expect(@storage.exists?(p_other)).to be_falsey
    @storage.include_query_string_in_uuid = false
    @storage.remove p_other
  end

  it 'should set page.fetched_at based on the id creation' do
    p = page_factory 'http://www.user-doojo.com'
    @storage.add p
    expect(p.fetched_at).to be_nil
    p = @storage.get p
    expect(p.fetched_at).not_to be_nil
    @storage.remove p
  end

  it 'should NOT set page.fetched_at if already present' do
    p = page_factory 'http://www.user-doojooo.com'
    p.fetched_at = 10
    @storage.add p
    p = @storage.get p
    expect(p.fetched_at).to be 10
    @storage.remove p
  end
end
