require 'faraday'
require 'json'
require 'logger'

class CoinglassAPI
  MAX_RETRIES = 3
  RETRY_DELAY = 5

  def initialize()
    @conn = Faraday.new(
      url: "https://open-api-v3.coinglass.com",
      headers: {
        'CG-API-KEY' => "#{ENV["COINGLASS_API_TOKEN"]}",
        'Accept' => 'application/json' },
      ssl: {
        verify: true
      },
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
    rescue => e
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

  def get_aggregated_open_interest(symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/openInterest/ohlc-aggregated-history"
    params = {
      symbol: symbol,
      interval: interval,
      start_time_seconds: start_time_seconds,
      start_time_seconds: end_time_seconds
    }.compact
    execute_request(:get, path, params)
  end

  def get_aggregated_funding_rates(symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/fundingRate/oi-weight-ohlc-history"
  end

  def get_accounts_long_short_ratio(exchange, symbol, interval, start_time_seconds, end_time_seconds)
    path = "/api/futures/globalLongShortAccountRatio/history"
  end

  def parse(response)
    JSON.parse(response.body)
  end
end
