require 'faraday'
require 'json'
require 'uri'
require 'openssl'
require 'base64'
require 'logger'

class LnMarketsAPI
  MAX_RETRIES = 3
  BASE_DELAY = 2 # second
  MAX_DELAY = 8 # seconds
  REQUEST_TIMEOUT = 30 # seconds
  RETRYABLE_ERRORS = [
    Faraday::ServerError,
    Faraday::ConnectionFailed,
    Faraday::TimeoutError,
    Faraday::ResourceNotFound,
    Faraday::ClientError,
    Net::ReadTimeout
  ]

  attr_reader :logger

  def initialize(log_level: Logger::DEBUG)
    @conn = create_connection
    @logger = Logger.new(STDOUT)
    @logger.level = log_level
  end

  private

  def create_connection
    Faraday.new(
      url: "https://api.lnmarkets.com",
      headers: {
        'LNM-ACCESS-KEY' => ENV.fetch("LNMARKETS_API_KEY"),
        'LNM-ACCESS-PASSPHRASE' => ENV.fetch("LNMARKETS_API_PASSPHRASE"),
        'Content-Type' => 'application/json'
      },
      ssl: { verify: true },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: REQUEST_TIMEOUT, open_timeout: REQUEST_TIMEOUT }
    ) do |faraday|
      faraday.response :raise_error
      faraday.adapter Faraday.default_adapter
    end
  end

  def execute_request(method, path, data = {})
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    caller_info = caller(1..1).first
    caller_method = caller_info[/`([^']*)'/, 1]
    retries = 0
    begin
      if retries > 0
        @logger.info("Retry request (#{retries}/#{MAX_RETRIES}) attempting...")
      end
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = (Time.now.to_f * 1000).to_i.to_s
      full_path = "/v2#{path}"

      signature = generate_signature(timestamp, method, full_path, data)

      @logger.debug("Request details:")
      @logger.debug("Method: #{method}")
      @logger.debug("Path: #{full_path}")
      @logger.debug("Data: #{data.inspect}")
      @logger.debug("Signature: #{signature}")

      response = @conn.send(method.downcase, full_path) do |req|
        req.headers['Content-Type'] = 'application/json'
        req.headers['LNM-ACCESS-SIGNATURE'] = signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp

        if ['GET', 'DELETE'].include?(method.upcase)
          req.params = data
        elsif ['POST', 'PUT'].include?(method.upcase)
          req.body = JSON.generate(data, { separators: [',', ':'] })
        end
      end

      elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
      hash_method_response = handle_response(response, elapsed_time)
    rescue *RETRYABLE_ERRORS => e
      if retries < MAX_RETRIES
        retries += 1
        delay = [BASE_DELAY * (2 ** (retries - 1)), MAX_DELAY].min
        @logger.warn("Request failed with #{e.class}. Retrying in #{delay} seconds (Attempt #{retries}/#{MAX_RETRIES})")
        sleep delay
        retry
      else
        hash_method_response = handle_error(e, Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time, caller_method)
      end
    rescue => e
      @logger.error("Error: #{e}")
      hash_method_response = handle_error(e, Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time, caller_method)
    end

    hash_method_response
  end

  def generate_signature(timestamp, method, path, data)
    params = ''

    if ['GET', 'DELETE'].include?(method.upcase)
      params = URI.encode_www_form(data.sort)
    elsif ['POST', 'PUT'].include?(method.upcase)
      params = JSON.generate(data, { separators: [',', ':'] })
    end

    payload = timestamp + method.upcase + path + params
    @logger.debug("Generating signature with payload: #{payload}")

    digest = OpenSSL::Digest.new('sha256')
    hmac = OpenSSL::HMAC.digest(digest, ENV.fetch("LNMARKETS_API_SECRET"), payload)
    Base64.strict_encode64(hmac)
  end

  def handle_response(response, elapsed_time)
    parsed_body = JSON.parse(response.body)
    
    response_object = {
      status: 'success',
      body: parsed_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    @logger.info("LnMarketsAPI Request successful. Status: #{response.status}, Elapsed time: #{elapsed_time.round(3)}s")
    @logger.debug("Full response object: #{JSON.pretty_generate(response_object)}")
    
    response_object
  end

  def handle_error(error, elapsed_time, caller_method)
    error_body = if error.respond_to?(:response) && error.response
                   JSON.parse(error.response[:body]) rescue error.response[:body]
                 else
                   nil
                 end
    error_message = error_body.is_a?(Hash) ? error_body['message'] : error.message
    
    error_response = {
      status: 'error',
      message: error_message,
      body: error_body,
      elapsed_time: elapsed_time.round(6),
      error_class: error.class.to_s
    }
    
    @logger.error("LnMarketsAPI Request failed. Error: #{error.class}, Message: #{error_message}, Elapsed time: #{elapsed_time.round(3)}s")
    @logger.debug("Full error response object: #{JSON.pretty_generate(error_response)}")

    # Push metric to Grafana Cloud
    push_error_metric(error, caller_method)
    
    error_response
  end

  def push_error_metric(error, caller_method)
    # Initialize GrafanaCloudInfluxPushAPI
    grafana_cloud_auth_token = Base64.strict_encode64('210825' + ':' + ENV['GRAFANA_CLOUD_METRICS_API_KEY'])
    @grafana_api = GrafanaCloudInfluxPushAPI.new(grafana_cloud_auth_token)

    # Configure metric payload
    timestamp = Time.now.to_i * 1_000_000_000 # Convert to nanoseconds
    metric = "lnmarkets_api_error,error_class=#{error.class},status_code=#{error.respond_to?(:response) ? error.response[:status].to_s : 'unknown'},method=#{caller_method} value=1 #{timestamp}"

    # Push metric
    @grafana_api.push_metrics(metric)
  end

  public

  # Options API methods
  def close_all_option_contracts
    execute_request('DELETE', '/options/close-all')
  end

  def close_options_contract(id)
    execute_request('DELETE', '/options', { id: id })
  end

  def open_option_contract(side, quantity, settlement, instrument_name)
    data = {
      side: side,
      quantity: quantity,
      settlement: settlement,
      instrument_name: instrument_name
    }
    execute_request('POST', '/options', data)
  end

  def get_options_instruments
    execute_request('GET', '/options/instruments')
  end

  def get_options_trades
    execute_request('GET', '/options')
  end

  def get_options_trade(trade_id)
    execute_request('GET', "/options/trades/#{trade_id}")
  end

  def get_options_instrument_volatility(instrument_name)
    execute_request('GET', '/options/instrument', { instrument_name: instrument_name })
  end

  def get_options_volatility_index
    execute_request('GET', '/options/volatility-index')
  end

  # User API methods
  def get_user_info
    execute_request('GET', '/user')
  end

  # Futures API methods
  def get_futures_trades(trade_type, from_time, to_time)
    data = { type: trade_type, from: from_time, to: to_time, limit: 1000 }
    execute_request('GET', '/futures', data)
  end

  def create_futures_trades(side, trade_type, leverage, price, quantity, takeprofit, stoploss)
    data = {
      side: side,
      type: trade_type,
      leverage: leverage,
      price: price,
      quantity: quantity,
      takeprofit: takeprofit,
      stoploss: stoploss
    }
    execute_request('POST', '/futures', data)
  end

  def close_all_futures_trades
    execute_request('DELETE', '/futures/all/close')
  end

  def close_futures_trade(trade_id)
    execute_request('DELETE', '/futures', { id: trade_id })
  end

  def cancel_futures_trade(trade_id)
    execute_request('POST', '/futures/cancel', { id: trade_id })
  end

  def update_futures_trade(id, trade_type, value)
    data = { id: id, type: trade_type, value: value }
    execute_request('PUT', '/futures', data)
  end

  def get_futures_trade(trade_id)
    execute_request('GET', "/futures/trades/#{trade_id}")
  end

  def get_price_btcusd_ticker
    execute_request('GET', "/futures/ticker")
  end

  # def get_price_btcusd_index_history(start_timestamp_seconds, end_timestamp_seconds)
  #   execute_request('GET', "/oracle/last-price", { from: start_timestamp_seconds, to: end_timestamp_seconds })
  # end
end
