require 'faraday'
class PolygonAPI

  def initialize()
    @conn = Faraday.new(
      url: "https://api.polygon.io/",
      headers: {
        'Authorization' => "Bearer #{ENV["POLYGON_API_TOKEN"]}",
        'Accept' => 'application/json' },
      ssl: {
        verify: true
      },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: 10 }
    ) do |faraday|
      faraday.response :raise_error
    end
  end

  def get_aggregate_bars(symbol, timespan, multiplier, start_date, end_date)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v2/aggs/ticker/#{symbol}/range/#{multiplier}/#{timespan}/#{start_date}/#{end_date}?sort=desc")
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
      puts "PolygonAPI Error!"
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

  def get_sma(symbol, timestamp, timespan, window, series_type)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/indicators/sma/#{symbol}", { timespan: timespan, 
        window: window, series_type: series_type, limit: 60, timestamp: timestamp })
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
      puts "PolygonAPI Error!"
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

  def get_ema(symbol, timestamp, timespan, window, series_type)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/indicators/ema/#{symbol}", { timespan: timespan, 
        window: window, series_type: series_type, limit: 60, timestamp: timestamp })
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
      puts "PolygonAPI Error!"
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

  def get_macd(symbol, timestamp, timespan, short_window, long_window, signal_window, series_type)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/indicators/macd/#{symbol}", { timespan: timespan, 
        short_window: short_window, long_window: long_window, 
        signal_window: signal_window, series_type: series_type,
        timestamp: timestamp })
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
      puts "PolygonAPI Error!"
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

  def get_rsi(symbol, timestamp, timespan, window, series_type)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/indicators/rsi/#{symbol}", { timespan: timespan, 
        window: window, series_type: series_type, timestamp: timestamp })
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
      puts "PolygonAPI Error!"
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

  def get_last_trade(symbol_from, symbol_to)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/last/crypto/#{symbol_from}/#{symbol_to}")
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
      puts "PolygonAPI Error!"
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

  def get_daily_open_close(symbol_from, symbol_to, date)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request_response = @conn.get("/v1/open-close/crypto/#{symbol_from}/#{symbol_to}/#{date}")
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
      puts "PolygonAPI Error!"
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