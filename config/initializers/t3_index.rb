require 'faraday'
class T3IndexAPI

  def initialize()
    @conn = Faraday.new(
      url: "https://crypto-volatility-index.p.rapidapi.com",
      headers: {
        'x-rapidapi-key' => "#{ENV["T3INDEX_API_TOKEN"]}",
        'x-rapidapi-host' => "crypto-volatility-index.p.rapidapi.com",
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

  def get_tick(date)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/tick/BTC/#{date}")
      time_finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      elapsed_time = (time_finish - time_start).round(6)
    rescue Faraday::ConnectionFailed => e
      puts e
      puts e.class
      puts e.inspect
      puts "Faraday Connection Failed Error!"

      hash_method_response[:status] = 'error'
      hash_method_response[:message] = 'ConnectionFailed'
      return hash_method_response
    rescue Faraday::ResourceNotFound => e
      puts e
      puts e.class
      puts e.inspect
      puts "Faraday ResourceNotFound error!"

      hash_method_response[:status] = 'error'
      hash_method_response[:message] = 'ResourceNotFound'
      return hash_method_response
    rescue Faraday::SSLError => e
      puts e
      puts e.class
      puts e.inspect
      puts "Faraday SSLError error!"

      hash_method_response[:status] = 'error'
      hash_method_response[:message] = 'SSLError'
      return hash_method_response
    rescue => e
      puts "T3IndexAPI Error!"
      puts e
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = 'Unable to fetch volatiltiy data from T3IndexAPI.'
      return hash_method_response
    else
      puts ''
      parsed_response_body = JSON.parse(request_response.body)

      hash_method_response[:status] = 'success'
      hash_method_response[:body] = parsed_response_body
      hash_method_response[:elapsed_time] = elapsed_time
      return hash_method_response
    end
  end

  def parse(response)
    JSON.parse(response.body)
  end
end