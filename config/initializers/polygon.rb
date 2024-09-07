require 'faraday'
require 'json'
require 'logger'

class PolygonAPI
  MAX_RETRIES = 3
  RETRY_DELAY = 10
  RETRYABLE_ERRORS = [
    Faraday::ServerError,
    Faraday::ConnectionFailed,
    Faraday::TimeoutError,
    Faraday::ResourceNotFound,
    Faraday::ClientError
  ]

  SKIP_LOGGING_METHODS = ['get_last_trade']


  attr_reader :logger

  def initialize(log_level: Logger::INFO)
    @conn = create_connection
    @logger = Logger.new(STDOUT)
    @logger.level = log_level
  end

  private

  def create_connection
    Faraday.new(
      url: "https://api.polygon.io",
      headers: {
        'Authorization' => "Bearer #{ENV.fetch("POLYGON_API_TOKEN")}",
        'Accept' => 'application/json'
      },
      ssl: { verify: true },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: 10 }
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
    rescue *RETRYABLE_ERRORS => e
      retries += 1
      if retries <= MAX_RETRIES
        @logger.warn("PolygonAPI Error: #{e.class} - #{e.message}. Retrying in #{RETRY_DELAY} seconds (Attempt #{retries}/#{MAX_RETRIES})")
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
    parsed_body = JSON.parse(response.body)
    
    full_response = {
      status: 'success',
      http_status: response.status,
      body: parsed_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    if SKIP_LOGGING_METHODS.include?(caller_method)
      @logger.info("PolygonAPI Request successful for #{caller_method}. Status: #{response.status}, Elapsed time: #{elapsed_time.round(3)}s")
    else
      @logger.info("PolygonAPI Response: #{JSON.pretty_generate(full_response)}")
    end
    
    full_response
  end

  def handle_error(error, start_time)
    elapsed_time = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time
    error_body = error.response ? (JSON.parse(error.response[:body]) rescue error.response[:body]) : nil
    error_message = error_body.is_a?(Hash) ? error_body['message'] : error.message
    
    full_error_response = {
      status: 'error',
      error_class: error.class.to_s,
      message: error_message,
      body: error_body,
      elapsed_time: elapsed_time.round(6)
    }
    
    @logger.error("PolygonAPI Error: #{JSON.pretty_generate(full_error_response)}")

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
    
    @logger.error("PolygonAPI Unexpected Error: #{JSON.pretty_generate(full_error_response)}")
    
    full_error_response
  end

  public

  def get_aggregate_bars(symbol, timespan, multiplier, start_date, end_date)
    path = "/v2/aggs/ticker/#{symbol}/range/#{multiplier}/#{timespan}/#{start_date}/#{end_date}"
    params = { sort: 'desc' }
    execute_request(:get, path, params)
  end

  def get_sma(symbol, timestamp, timespan, window, series_type)
    path = "/v1/indicators/sma/#{symbol}"
    params = {
      timespan: timespan,
      window: window,
      series_type: series_type,
      limit: 60,
      timestamp: timestamp
    }
    execute_request(:get, path, params)
  end

  def get_ema(symbol, timestamp, timespan, window, series_type)
    path = "/v1/indicators/ema/#{symbol}"
    params = {
      timespan: timespan,
      window: window,
      series_type: series_type,
      limit: 60,
      timestamp: timestamp
    }
    execute_request(:get, path, params)
  end

  def get_macd(symbol, timestamp, timespan, short_window, long_window, signal_window, series_type)
    path = "/v1/indicators/macd/#{symbol}"
    params = {
      timespan: timespan,
      short_window: short_window,
      long_window: long_window,
      signal_window: signal_window,
      series_type: series_type,
      timestamp: timestamp
    }
    execute_request(:get, path, params)
  end

  def get_rsi(symbol, timestamp, timespan, window, series_type)
    path = "/v1/indicators/rsi/#{symbol}"
    params = {
      timespan: timespan,
      window: window,
      series_type: series_type,
      timestamp: timestamp
    }
    execute_request(:get, path, params)
  end

  def get_last_trade(symbol_from, symbol_to)
    path = "/v1/last/crypto/#{symbol_from}/#{symbol_to}"
    execute_request(:get, path)
  end

  def get_daily_open_close(symbol_from, symbol_to, date)
    path = "/v1/open-close/crypto/#{symbol_from}/#{symbol_to}/#{date}"
    execute_request(:get, path)
  end

  def get_trades(symbol)
    path = "/v3/trades/#{symbol}"
    params = { limit: 100 }
    execute_request(:get, path, params)
  end
end
