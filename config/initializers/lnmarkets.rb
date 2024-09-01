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

  def initialize(log_level: Logger::INFO)
    @conn = create_connection
    @logger = Logger.new(STDOUT)
    @logger.level = log_level
  end

  private

  def create_connection
    Faraday.new(
      url: "https://api.lnmarkets.com/v2",
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

  def execute_request(method, path, params = {}, body = nil)
    retries = 0
    start_time = Time.now

    begin
      timestamp = (Time.now.to_f * 1000).to_i.to_s
      signature = generate_signature(timestamp, method, path, params, body)

      response = @conn.send(method.downcase) do |req|
        req.url path
        req.params.merge!(params) if !params.empty?
        req.body = body.to_json if body
        req.headers['LNM-ACCESS-SIGNATURE'] = signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end

      handle_response(response, Time.now - start_time)
    rescue *RETRYABLE_ERRORS => e
      if retries < MAX_RETRIES
        retries += 1
        @logger.warn("Request failed with #{e.class}. Retrying in #{RETRY_DELAY} seconds (Attempt #{retries}/#{MAX_RETRIES})")
        sleep RETRY_DELAY
        retry
      else
        handle_error(e, Time.now - start_time)
      end
    rescue => e
      handle_error(e, Time.now - start_time)
    end
  end

  def generate_signature(timestamp, method, path, params, body)
    query_string = URI.encode_www_form(params.sort)
    payload = timestamp + method.upcase + path + query_string + (body ? body.to_json : '')
    
    @logger.debug("Generating signature with payload: #{payload}")
    
    digest = OpenSSL::Digest.new('sha256')
    hmac = OpenSSL::HMAC.digest(digest, ENV.fetch("LNMARKETS_API_SECRET"), payload)
    Base64.strict_encode64(hmac)
  end

  def handle_response(response, elapsed_time)
    @logger.info("Request successful. Status: #{response.status}, Elapsed time: #{elapsed_time.round(3)}s")
    {
      status: 'success',
      body: JSON.parse(response.body),
      elapsed_time: elapsed_time.round(6)
    }
  end

  def handle_error(error, elapsed_time)
    error_body = error.respond_to?(:response) ? (JSON.parse(error.response[:body]) rescue error.response[:body]) : nil
    error_message = error_body.is_a?(Hash) ? error_body['message'] : error.message
    
    @logger.error("Request failed. Error: #{error.class}, Message: #{error_message}, Elapsed time: #{elapsed_time.round(3)}s")
    
    {
      status: 'error',
      message: error_message,
      body: error_body,
      elapsed_time: elapsed_time.round(6)
    }
  end

  public

  # Options API methods
  def close_all_option_contracts
    execute_request('DELETE', '/options/close-all')
  end

  def close_options_contract(trade_id)
    execute_request('DELETE', '/options', { id: trade_id })
  end

  def open_option_contract(side, quantity, settlement, instrument_name)
    body = {
      side: side,
      quantity: quantity,
      settlement: settlement,
      instrument_name: instrument_name
    }
    execute_request('POST', '/options', {}, body)
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
    params = { type: trade_type, from: from_time, to: to_time, limit: 1000 }
    execute_request('GET', '/futures', params)
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
    execute_request('POST', '/futures', {}, body)
  end

  def close_all_futures_trades
    execute_request('DELETE', '/futures/all/close')
  end

  def close_futures_trade(trade_id)
    execute_request('DELETE', '/futures', { id: trade_id })
  end

  def cancel_futures_trade(trade_id)
    execute_request('POST', '/futures/cancel', {}, { id: trade_id })
  end

  def update_futures_trade(id, trade_type, value)
    body = { id: id, type: trade_type, value: value }
    execute_request('PUT', '/futures', {}, body)
  end

  def get_futures_trade(trade_id)
    execute_request('GET', "/futures/trades/#{trade_id}")
  end

  def get_price_btcusd_ticker
    execute_request('GET', "/futures/ticker")
  end
end
