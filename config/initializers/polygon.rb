require 'faraday'
require 'json'
require 'logger'

class CoinglassAPI
  MAX_RETRIES = 3
  RETRY_DELAY = 10
  RETRYABLE_ERRORS = [
    Faraday::ServerError,
    Faraday::ConnectionFailed,
    Faraday::TimeoutError,
    Faraday::ResourceNotFound,
    Faraday::ClientError
  ]

  attr_reader :logger

  def initialize(log_level: Logger::INFO)
    @conn = Faraday.new(
      url: "https://open-api-v3.coinglass.com",
      headers: {
        'CG-API-KEY' => "#{ENV["COINGLASS_API_TOKEN"]}",
        'Accept' => 'application/json'
      },
      ssl: { verify: true },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: 30 }
    ) do |faraday|
      faraday.response :raise_error
    end
    @logger = Logger.new(STDOUT)
    @logger.level = log_level
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
    rescue *RETRYABLE_ERRORS => e
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

  def handle_response(response, start_time, caller_method:)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    parsed_body = parse(response)
    
    full_response = {
      status: 'success',
      http_status: response.status,
      body: parsed_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    @logger.info("CoinglassAPI Response for #{caller_method}: #{JSON.pretty_generate(full_response)}")
    
    full_response
  end

  def handle_error(error, start_time)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    error_body = error.response ? (parse(error.response) rescue error.response[:body]) : nil
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

  def get_aggregated_open_interest(symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/openInterest/ohlc-aggregated-history"
    params = {
      symbol: symbol,
      interval: interval,
      startTime: start_time_seconds,
      endTime: end_time_seconds
    }.compact
    execute_request(:get, path, params)
  end

  def get_aggregated_funding_rates(symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/fundingRate/oi-weight-ohlc-history"
    params = {
      symbol: symbol,
      interval: interval,
      startTime: start_time_seconds,
      endTime: end_time_seconds
    }.compact
    execute_request(:get, path, params)
  end

  def get_accounts_long_short_ratio(exchange, symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/globalLongShortAccountRatio/history"
    params = {
      exchange: exchange,
      symbol: symbol,
      interval: interval,
      startTime: start_time_seconds,
      endTime: end_time_seconds
    }.compact
    execute_request(:get, path, params)
  end

  def parse(response)
    JSON.parse(response.body)
  end
end