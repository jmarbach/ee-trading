require 'faraday'
class CoinglassAPI

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

  def get_aggregated_open_interest(symbol, interval, start_time_seconds, end_time_seconds)
  end

  def get_aggregated_funding_rates(symbol, interval, start_time_seconds, end_time_seconds)
  end

  def get_accounts_long_short_ratio(exchange, symbol, interval, start_time_seconds, end_time_seconds)
  end

  def parse(response)
    JSON.parse(response.body)
  end
end
