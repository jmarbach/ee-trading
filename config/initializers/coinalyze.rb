require 'faraday'
class CoinalyzeAPI

  def initialize()
    @conn = Faraday.new(
      url: "https://api.coinalyze.net/v1/",
      headers: {
        'api_key' => "#{ENV["COINALYZE_API_TOKEN"]}",
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

  def get_avg_funding_history(symbol, interval, start_time, end_time)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("funding-rate-history?symbols=#{symbol}&interval=#{interval}&from=#{start_time}&to=#{end_time}")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_funding_rate
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("funding-rate?symbols=BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_spot_markets
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("spot-markets")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_future_markets
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("future-markets")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_avg_open_interest_history(symbol, interval, start_time, end_time)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("open-interest-history?symbols=#{symbol}&interval=#{interval}&from=#{start_time}&to=#{end_time}")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_open_interest
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("open-interest?symbols=BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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

  def get_long_short_ratio_history(symbol, interval, start_time, end_time)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("long-short-ratio-history?symbols=#{symbol}&interval=#{interval}&from=#{start_time}&to=#{end_time}")
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
      puts "Coinalyze Error!"
      puts e
      puts e.response
      hash_method_response[:status] = 'error'
      if e.response != nil
        parsed_response_body = JSON.parse(e.response[:body])
        hash_method_response[:message] = parsed_response_body['message']
      end
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