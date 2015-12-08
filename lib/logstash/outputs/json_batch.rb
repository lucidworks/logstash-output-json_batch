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
  config :url, :validate => :string, :default => "http://localhost:8764/api/apollo/index-pipelines/conn_solr/collections/logstash/index" 

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash

  def register
    # Handle this deprecated option. TODO: remove the option
    #@ssl_certificate_validation = @verify_ssl if @verify_ssl

    # We count outstanding requests with this queue
    # This queue tracks the requests to create backpressure
    # When this queue is empty no new requests may be sent,
    # tokens must be added back by the client on success
    @request_tokens = SizedQueue.new(@pool_max)
    @pool_max.times {|t| @request_tokens << true }

    @requests = Array.new

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
    puts @flush_size

  end # def register

  def receive(event)
    buffer_receive(event)
  end #def event

  public
  def flush(events, close=false)
    documents = []  #this is the array of hashes that we push to Fusion as documents

    events.each do |event|
        document = event.to_hash()
        documents.push(document)
    end

    make_request(documents, 0)
  end

  private

  def make_request(documents, count)
    body = LogStash::Json.dump(documents)
    # Block waiting for a token
    token = @request_tokens.pop

    # Create an async request
    begin
      request = client.send(:post, @url, :body => body, :headers => request_headers, :async => true)
    rescue Exception => e
      @logger.warn("An error occurred while indexing: #{e.message}")
    end

    # with Maticore version < 0.5 using :async => true places the requests in an @async_requests
    # list which is used & cleaned by Client#execute! but we are not using it here and we must
    # purge it manually to avoid leaking requests.
    client.clear_pending

    # attach handlers before performing request
    request.on_complete do
      # Make sure we return the token to the pool
      @request_tokens << token
    end

    request.on_success do |response|

      #string = "Some "+ Time.new.inspect + " " + response
      #puts "%s status code returned for %s docs @ %s\n" % [response.code, documents.length, Time.new.inspect]
      if response.code < 200 || response.code > 299
        log_failure(
          "Encountered non-200 HTTP code #{200}",
          :response_code => response.code,
          :url => url,
          :event => event)
      end
    end

    request.on_failure do |exception|
      if count < 1000
        # Workaround due to Manticore sometimes trying to reuse stale connections after idling, 
        # essentially all threads will fail once and then it will succeeed. 
        # TODO: better http client
        sleep 0.1
        make_request(documents, count + 1)
      else
        log_failure("Could not fetch URL",
          :url => url,
          :method => @http_method,
          :body => body,
          :headers => headers,
          :message => exception.message,
          :class => exception.class.name,
          :backtrace => exception.backtrace
        )
      end
    end

    # Invoke it using the Manticore Executor (CachedThreadPool) directly
    begin
      request_async_background(request)
    rescue Exception => e
      puts "Hello!"
      @logger.warn("An error occurred while indexing: #{e.message}")
    end
  end

  # This is split into a separate method mostly to help testing
  def log_failure(message, opts)
    @logger.error("[HTTP Output Failure] #{message}", opts)
  end

  # Manticore doesn't provide a way to attach handlers to background or async requests well
  # It wants you to use futures. The #async method kinda works but expects single thread batches
  # and background only returns futures.
  # Proposed fix to manticore here: https://github.com/cheald/manticore/issues/32
  def request_async_background(request)
    @method ||= client.executor.java_method(:submit, [java.util.concurrent.Callable.java_class])
    @method.call(request)
  end

  def request_headers()
    headers = @headers || {}
    headers["Content-Type"] ||= "application/json"
    headers
  end

end
