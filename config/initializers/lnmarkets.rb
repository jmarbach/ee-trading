require 'faraday'
require 'json'
require 'uri'
require 'openssl'
require 'base64'
require 'logger'

class LnMarketsAPI
  MAX_RETRIES = 3
  RETRY_DELAY = 10 # seconds
  RETRYABLE_ERRORS = [Faraday::ServerError, Faraday::ConnectionFailed]

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
      request: { timeout: 60 }
    ) do |faraday|
      faraday.response :raise_error
      faraday.adapter Faraday.default_adapter
    end
  end

  def generate_signature(timestamp, method, path, params_or_body)
    query_string = if method == 'GET'
                     URI.encode_www_form(params_or_body.sort)
                   else
                     params_or_body.to_json
                   end
    prehash_string = timestamp + method + path + query_string
    @logger.debug("Generating signature with payload: #{prehash_string}")
    
    digest = OpenSSL::Digest.new('sha256')
    hmac = OpenSSL::HMAC.digest(digest, ENV.fetch("LNMARKETS_API_SECRET"), prehash_string)
    Base64.strict_encode64(hmac)
  end

  def execute_request(method, path, params = {}, body = nil)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    retries = 0
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = (Time.now.to_f * 1000).to_i.to_s

      signature = generate_signature(timestamp, method, path, method == 'GET' ? params : (body || {}))

      @logger.debug("Request details:")
      @logger.debug("Method: #{method}")
      @logger.debug("Path: #{path}")
      @logger.debug("Params: #{params}")
      @logger.debug("Body: #{body}")
      @logger.debug("Signature: #{signature}")

      response = @conn.send(method.downcase) do |req|
        req.url path
        req.params = params if method == 'GET'
        req.body = body.to_json if body && !body.empty?
        req.headers['LNM-ACCESS-SIGNATURE'] = signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end

      time_finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      elapsed_time = (time_finish - time_start).round(6)

      hash_method_response = handle_response(response, elapsed_time)
    rescue *RETRYABLE_ERRORS => e
      if retries < MAX_RETRIES
        retries += 1
        @logger.warn("Request failed with #{e.class}. Retrying in #{RETRY_DELAY} seconds (Attempt #{retries}/#{MAX_RETRIES})")
        sleep RETRY_DELAY
        retry
      else
        hash_method_response = handle_error(e, Time.now - time_start)
      end
    rescue Faraday::ResourceNotFound => e
      @logger.error("ResourceNotFound error: #{e.message}")
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = 'ResourceNotFound'
    rescue => e
      hash_method_response = handle_error(e, Time.now - time_start)
    end

    hash_method_response
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

  def handle_error(error, elapsed_time)
    error_body = error.respond_to?(:response) ? (JSON.parse(error.response[:body]) rescue error.response[:body]) : nil
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
    
    error_response
  end

  public

  # Options API methods
  def close_all_option_contracts
    execute_request('DELETE', '/v2/options/close-all')
  end

  def close_options_contract(trade_id)
    execute_request('DELETE', '/v2/options', { id: trade_id })
  end

  def open_option_contract(side, quantity, settlement, instrument_name)
    body = {
      side: side,
      quantity: quantity,
      settlement: settlement,
      instrument_name: instrument_name
    }
    execute_request('POST', '/v2/options', {}, body)
  end

  def get_options_instruments
    execute_request('GET', '/v2/options/instruments')
  end

  def get_options_trades
    execute_request('GET', '/v2/options')
  end

  def get_options_trade(trade_id)
    execute_request('GET', "/v2/options/trades/#{trade_id}")
  end

  def get_options_instrument_volatility(instrument_name)
    execute_request('GET', '/v2/options/instrument', { instrument_name: instrument_name })
  end

  def get_options_volatility_index
    execute_request('GET', '/v2/options/volatility-index')
  end

  # User API methods
  def get_user_info
    execute_request('GET', '/v2/user')
  end

  # Futures API methods
  def get_futures_trades(trade_type, from_time, to_time)
    params = { type: trade_type, from: from_time, to: to_time, limit: 1000 }
    execute_request('GET', '/v2/futures', params)
  end

  def create_futures_trades(side, trade_type, leverage, price, quantity, takeprofit, stoploss)
    body = {
      side: side,
      type: trade_type,
      leverage: leverage,
      price: price,
      quantity: quantity,
      takeprofit: takeprofit,
      stoploss: stoploss
    }
    execute_request('POST', '/v2/futures', {}, body)
  end

  def close_all_futures_trades
    execute_request('DELETE', '/v2/futures/all/close')
  end

  def close_futures_trade(trade_id)
    execute_request('DELETE', '/v2/futures', { id: trade_id })
  end

  def cancel_futures_trade(trade_id)
    execute_request('POST', '/v2/futures/cancel', {}, { id: trade_id })
  end

  def update_futures_trade(id, trade_type, value)
    body = { id: id, type: trade_type, value: value }
    execute_request('PUT', '/v2/futures', {}, body)
  end

  def get_futures_trade(trade_id)
    execute_request('GET', "/v2/futures/trades/#{trade_id}")
  end

  def get_price_btcusd_ticker
    execute_request('GET', "/v2/futures/ticker")
  end
end
