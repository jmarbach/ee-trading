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
    validate_options_params(data)
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

  def create_deposit(amount)
    data = { amount: amount }
    validate_deposit_params(data)
    execute_request('POST', '/user/deposit', data)
  end

  # Futures API methods
  def get_futures_trades(trade_type, from_time, to_time)
    data = { 
      type: trade_type, 
      from: from_time, 
      to: to_time, 
      limit: 1000 
    }
    validate_timestamp_params(data)
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
    validate_futures_trade_params(data)
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
    data = { 
      id: id, 
      type: trade_type, 
      value: value
    }
    validate_futures_update_params(data)
    execute_request('PUT', '/futures', data)
  end

  def get_futures_trade(trade_id)
    execute_request('GET', "/futures/trades/#{trade_id}")
  end

  def get_price_btcusd_ticker
    execute_request('GET', "/futures/ticker")
  end

  def get_open_futures_trades
    @logger.info("Fetching open futures trades")
    response = execute_request('GET', '/futures', { type: 'open' })
    if response[:status] == 'success'
      @logger.info("Successfully fetched running futures trades")
      response[:body]
    else
      @logger.error("Failed to fetch open futures trades. Error: #{response[:message]}")
      []
    end
  end

  private

  def handle_numeric_param(value, param_name)
    return value if value.is_a?(Integer)
    
    if value.is_a?(Float)
      rounded = value.round
      @logger.warn("Converting float #{param_name} #{value} to integer #{rounded}")
      rounded
    else
      # Try to convert string to number if needed
      begin
        num = Float(value)
        rounded = num.round
        @logger.warn("Converting #{param_name} #{value} (#{value.class}) to integer #{rounded}")
        rounded
      rescue ArgumentError, TypeError
        raise ArgumentError, "#{param_name} must be a number"
      end
    end
  end

  def validate_futures_trade_params(data)
    # Handle quantity
    data[:quantity] = handle_numeric_param(data[:quantity], 'quantity')
    raise ArgumentError, "Quantity must be positive" unless data[:quantity].positive?
    
    # Handle leverage
    data[:leverage] = handle_numeric_param(data[:leverage], 'leverage')
    raise ArgumentError, "Leverage must be positive" unless data[:leverage].positive?
    
    # Handle price
    data[:price] = handle_numeric_param(data[:price], 'price')
    raise ArgumentError, "Price must be positive" unless data[:price].positive?
    
    # Handle takeprofit
    data[:takeprofit] = handle_numeric_param(data[:takeprofit], 'takeprofit')
    
    # Handle stoploss
    data[:stoploss] = handle_numeric_param(data[:stoploss], 'stoploss')
    
    # Validate side
    unless %w[b s].include?(data[:side])
      raise ArgumentError, "Side must be either 'b' (long) or 's' (short)"
    end
    
    # Validate type
    unless %w[l m].include?(data[:type])
      raise ArgumentError, "Type must be either 'l' (limit) or 'm' (market)"
    end
  end

  def validate_options_params(data)
    # Handle quantity
    data[:quantity] = handle_numeric_param(data[:quantity], 'quantity')
    raise ArgumentError, "Quantity must be positive" unless data[:quantity].positive?

    # Validate side
    unless %w[b s].include?(data[:side])
      raise ArgumentError, "Side must be either 'b' (buy) or 's' (sell)"
    end

    # Handle settlement if present
    if data[:settlement]
      data[:settlement] = handle_numeric_param(data[:settlement], 'settlement')
      raise ArgumentError, "Settlement must be positive" unless data[:settlement].positive?
    end
  end

  def validate_deposit_params(data)
    data[:amount] = handle_numeric_param(data[:amount], 'amount')
    raise ArgumentError, "Deposit amount must be positive" unless data[:amount].positive?
  end

  def validate_futures_update_params(data)
    data[:value] = handle_numeric_param(data[:value], 'value')

    unless %w[takeprofit stoploss].include?(data[:type])
      raise ArgumentError, "Update type must be either 'takeprofit' or 'stoploss'"
    end
  end

  def validate_timestamp_params(data)
    %i[from to].each do |key|
      if data[key]
        data[key] = handle_numeric_param(data[key], key.to_s)
        raise ArgumentError, "#{key} timestamp must be positive" unless data[key].positive?
      end
    end
  end

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
    start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    begin
      if retries > 0
        @logger.info("Retry request (#{retries}/#{MAX_RETRIES}) attempting...")
      end

      timestamp = (Time.now.to_f * 1000).to_i.to_s
      full_path = "/v2#{path}"
      signature = generate_signature(timestamp, method, full_path, data)

      @logger.debug("Request details:")
      @logger.debug("Method: #{method}")
      @logger.debug("Path: #{full_path}")
      @logger.debug("Data: #{data.inspect}")
      @logger.debug("Signature: #{signature}")

      response = @conn.send(method.downcase, full_path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp

        if ['GET', 'DELETE'].include?(method.upcase)
          req.params = data
        elsif ['POST', 'PUT'].include?(method.upcase)
          req.body = JSON.generate(data)
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
      hash_method_response = handle_error(e, Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time, caller_method)
    end

    hash_method_response
  end

  def generate_signature(timestamp, method, path, data)
    params = ''

    if ['GET', 'DELETE'].include?(method.upcase)
      params = URI.encode_www_form(data.sort)
    elsif ['POST', 'PUT'].include?(method.upcase)
      params = JSON.generate(data)
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
      error_class: error.class.to_s,
      response_status: error.respond_to?(:response) ? error.response[:status] : nil,
      response_headers: error.respond_to?(:response) ? error.response[:headers] : nil
    }
    
    @logger.error("LnMarketsAPI Request failed. Error: #{error.class}, Message: #{error_message}, Status: #{error_response[:response_status]}, Elapsed time: #{elapsed_time.round(3)}s")
    @logger.debug("Full error response object: #{JSON.pretty_generate(error_response)}")

    # Push metric to Grafana Cloud
    push_error_metric(error, caller_method)
    
    error_response
  end

  def push_error_metric(error, caller_method)
    # Initialize GrafanaCloudInfluxPushAPI
    grafana_cloud_auth_token = Base64.strict_encode64('210825' + ':' + ENV['GRAFANA_CLOUD_METRICS_API_KEY'].to_s)
    @grafana_api = GrafanaCloudInfluxPushAPI.new(grafana_cloud_auth_token)

    # Configure metric payload
    timestamp = Time.now.to_i * 1_000_000_000 # Convert to nanoseconds
    error_class = error.class.to_s
    status_code = error.respond_to?(:response) && error.response ? error.response[:status].to_s : 'unknown'
    
    metric = "lnmarkets_api_error,error_class=#{error_class},status_code=#{status_code},method=#{caller_method} value=1 #{timestamp}"

    # Push metric
    @grafana_api.push_metrics(metric)
  rescue => e
    @logger.error("Failed to push error metric: #{e.message}")
  end
end
