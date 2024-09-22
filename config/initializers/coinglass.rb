require 'faraday'
require 'json'
require 'logger'

class CoinglassAPI
  MAX_RETRIES = 3
  RETRY_DELAY = 5
  SKIP_LOGGING_METHODS = [] # Add method names here if you want to skip detailed logging for certain methods

  def initialize(log_level: Logger::INFO)
    @logger = Logger.new(STDOUT)
    @logger.level = log_level
    @conn = Faraday.new(
      url: "https://open-api-v3.coinglass.com",
      headers: {
        'CG-API-KEY' => ENV["COINGLASS_API_TOKEN"],
        'Accept' => 'application/json'
      },
      ssl: { verify: true },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: 30 }
    ) do |faraday|
      faraday.response :raise_error
    end
  end

  def execute_request(method, path, params = {})
    retries = 0
    begin
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      full_path = path
      full_path += "?#{URI.encode_www_form(params)}" if method.to_s.downcase == 'get' && !params.empty?

      @logger.debug("Executing #{method.upcase} request to #{full_path}")
      
      response = @conn.send(method.downcase) do |req|
        req.url full_path
        if method.to_s.downcase != 'get' && !params.empty?
          req.headers['Content-Type'] = 'application/json'
          req.body = params.to_json
        end
      end
      
      handle_response(response, start_time, caller_method: caller_locations(1,1)[0].label)
    rescue Faraday::ClientError, Faraday::ServerError => e
      retries += 1
      if retries <= MAX_RETRIES
        @logger.warn("CoinglassAPI Error: #{e.class} - #{e.message}. Retrying in #{RETRY_DELAY} seconds (Attempt #{retries}/#{MAX_RETRIES})")
        sleep RETRY_DELAY
        retry
      else
        handle_error(e, start_time)
      end
    rescue => e
      handle_unexpected_error(e, start_time)
    end
  end

  def get_aggregated_open_interest(symbol, interval, start_time = nil, end_time = nil, limit = nil)
    path = "/api/futures/openInterest/ohlc-aggregated-history"
    params = {
      symbol: symbol,
      interval: interval,
      startTime: start_time,
      endTime: end_time,
      limit: limit
    }.compact
    execute_request(:get, path, params)
  end

  def get_aggregated_funding_rates(symbol, interval, start_time = nil, end_time = nil, limit = nil)
    path = "/api/futures/fundingRate/oi-weight-ohlc-history"
    params = {
      symbol: symbol,
      interval: interval,
      startTime: start_time,
      endTime: end_time,
      limit: limit
    }.compact
    execute_request(:get, path, params)
  end

  def get_accounts_long_short_ratio(exchange, symbol, interval, start_time = nil, end_time = nil, limit = nil)
    path = "/api/futures/globalLongShortAccountRatio/history"
    params = {
      exchange: exchange,
      symbol: symbol,
      interval: interval,
      startTime: start_time,
      endTime: end_time,
      limit: limit
    }.compact
    execute_request(:get, path, params)
  end

  private

  def handle_response(response, start_time, caller_method:)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    parsed_body = JSON.parse(response.body)
    
    full_response = {
      status: parsed_body['success'] == false ? 'error' : 'success',
      http_status: response.status,
      body: parsed_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    if SKIP_LOGGING_METHODS.include?(caller_method)
      @logger.info("CoinglassAPI Request successful for #{caller_method}. Status: #{response.status}, Elapsed time: #{elapsed_time.round(3)}s")
    else
      @logger.info("CoinglassAPI Response: #{JSON.pretty_generate(full_response)}")
    end
    
    full_response
  end

  def handle_error(error, start_time)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    error_body = error.response ? (JSON.parse(error.response[:body]) rescue error.response[:body]) : nil
    error_message = error_body.is_a?(Hash) ? error_body['msg'] : error.message
    
    full_error_response = {
      status: 'error',
      error_class: error.class.to_s,
      message: error_message,
      body: error_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    @logger.error("CoinglassAPI Error: #{JSON.pretty_generate(full_error_response)}")

    full_error_response
  end

  def handle_unexpected_error(error, start_time)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    
    full_error_response = {
      status: 'error',
      error_class: error.class.to_s,
      message: "Unexpected error: #{error.message}",
      elapsed_time: elapsed_time.round(6)
    }
    
    @logger.error("CoinglassAPI Unexpected Error: #{JSON.pretty_generate(full_error_response)}")
    
    full_error_response
  end
end
