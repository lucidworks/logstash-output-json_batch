# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "uri"
require "stud/buffer"
require "logstash/plugin_mixins/http_client"

class LogStash::Outputs::JSONBatch < LogStash::Outputs::Base
  include LogStash::PluginMixins::HttpClient
  include Stud::Buffer

  config_name "json_batch"

  # URL to use
  config :url, :validate => :string, :required => true 

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash

  config :flush_size, :validate => :number, :default => 50

  config :idle_flush_time, :validate => :number, :default => 5

  config :retry_individual, :validate => :boolean, :default => true

  config :pool_max, :validate => :number, :default => 50

  def register
    # Handle this deprecated option. TODO: remove the option
    #@ssl_certificate_validation = @verify_ssl if @verify_ssl

    # We count outstanding requests with this queue
    # This queue tracks the requests to create backpressure
    # When this queue is empty no new requests may be sent,
    # tokens must be added back by the client on success
    @request_tokens = SizedQueue.new(@pool_max)
    @pool_max.times {|t| @request_tokens << true }
    @total = 0
    @total_failed = 0
    @requests = Array.new

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
    logger.info("Initialized json_batch with settings", 
      :flush_size => @flush_size,
      :idle_flush_time => @idle_flush_time,
      :request_tokens => @pool_max,
      :url => @url,
      :headers => request_headers,
      :retry_individual => @retry_individual)

  end # def register

  # This module currently does not support parallel requests as that would circumvent the batching
  def receive(event, async_type=:background)
    buffer_receive(event)
  end #def event

  public
  def flush(events, close=false)
    documents = []  #this is the array of hashes that we push to Fusion as documents

    events.each do |event|
        document = event.to_hash()
        documents.push(document)
    end

    make_request(documents)
  end

  def multi_receive(events)
    events.each {|event| buffer_receive(event)}
  end

  private

  def make_request(documents)
    body = LogStash::Json.dump(documents)
    # Block waiting for a token
    token = @request_tokens.pop

    # Create an async request
    begin
      request = client.send(:post, @url, :body => body, :headers => request_headers, :async => true)
    rescue Exception => e
      @logger.warn("An error occurred while indexing: #{e.message}")
    end

    # attach handlers before performing request
    request.on_complete do
      # Make sure we return the token to the pool
      @request_tokens << token
    end

    request.on_success do |response|
      if response.code >= 200 && response.code < 300
        @total = @total + documents.length
        logger.debug("Successfully submitted", 
          :docs => documents.length,
          :response_code => response.code,
          :total => @total)
      else
        if documents.length > 1 && @retry_individual
          documents.each do |doc| 
            make_request([doc])
          end
        else 
          @total_failed += documents.length
          log_failure(
              "Encountered non-200 HTTP code #{response.code}",
              :response_code => response.code,
              :url => url,
              :response_body => response.body,
              :num_docs => documents.length,
              :retry_individual => @retry_individual,
              :total_failed => @total_failed)
        end
      end
    end

    request.on_failure do |exception|
      @total_failed += documents.length
      log_failure("Could not access URL",
        :url => url,
        :method => @http_method,
        :body => body,
        :headers => headers,
        :message => exception.message,
        :class => exception.class.name,
        :backtrace => exception.backtrace,
        :total_failed => @total_failed
      )
    end

    client.execute!
  end

  # This is split into a separate method mostly to help testing
  def log_failure(message, opts)
    @logger.error("[HTTP Output Failure] #{message}", opts)
  end

  def request_headers()
    headers = @headers || {}
    headers["Content-Type"] ||= "application/json"
    headers
  end

end
