require 'faraday'

class LnmarketsAPI

  def initialize()
    @conn = Faraday.new(
      url: "https://api.lnmarkets.com",
      headers: {
        'LNM-ACCESS-KEY' => "#{ENV["LNMARKETS_API_KEY"]}",
        'LNM-ACCESS-PASSPHRASE' => "#{ENV["LNMARKETS_API_PASSPHRASE"]}",
        'Content-Type' => 'application/json' },
      ssl: {
        verify: true
      },
      proxy: ENV["SQUID_PROXY_URL"],
      request: { timeout: 60 }
    ) do |faraday|
      faraday.response :raise_error
    end
  end

  def close_all_option_contracts
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options/close-all'
      params = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'DELETE' + path + params )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.delete(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def close_options_contract(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options'
      hash_params = { id: trade_id }
      data = URI.encode_www_form(hash_params)

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'DELETE' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.delete(path, hash_params) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def open_option_contract(side, quantity, settlement, instrument_name)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      path = '/options'
      payload = "{\"side\":\"#{side}\",\"quantity\":#{quantity},\"settlement\":\"#{settlement}\",\"instrument_name\":\"#{instrument_name}\"}"
      timestamp = (Time.now.to_i * 1000).to_s
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], "#{timestamp}POST/v2#{path}#{payload}")
      signature = Base64.strict_encode64(hmac)

      request_response = @conn.post("v2#{path}") do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
        req.body = payload
      end

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
      puts "LnmarketsAPI Error!"
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

  def get_options_instruments
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options/instruments'
      params = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + params )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      puts e
      hash_method_response[:status] = 'error'
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

  def get_options_trades()
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options'

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def get_options_trade(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = "/v2/options/trades/#{trade_id}"
      data = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      hash_method_response[:status] = 'error'
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

  def get_options_instrument_volatility(instrument_name)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options/instrument'
      hash_params = { instrument_name: instrument_name }
      data = URI.encode_www_form(hash_params)

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path, hash_params) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def get_options_volatility_index
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options/volatility-index'
      params = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + params )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def get_user_info
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/user'
      params = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + params )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      puts e
      hash_method_response[:status] = 'error'
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

  def get_futures_trades(trade_type)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/futures'
      hash_params = { type: trade_type }
      data = URI.encode_www_form(hash_params)

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + data)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path, hash_params) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      puts e
      hash_method_response[:message] = parsed_response_body['message']
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

  def create_futures_trades(side, trade_type, leverage, price, quantity, takeprofit, stoploss)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = (Time.now.to_f * 1000).to_i.to_s
      path = '/futures'
      hash_params = { 
        side: side, 
        type: trade_type, 
        leverage: leverage, 
        price: price, 
        quantity: quantity, 
        takeprofit: takeprofit, 
        stoploss: stoploss 
      }
      json_body = hash_params.to_json

      digest = OpenSSL::Digest.new('sha256')
      payload = timestamp + 'POST' + '/v2' + path + json_body
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], payload)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.post("/v2#{path}") do |req|
        req.headers['Content-Type'] = 'application/json'
        req.headers['LNM-ACCESS-KEY'] = ENV["LNMARKETS_API_KEY"]
        req.headers['LNM-ACCESS-PASSPHRASE'] = ENV["LNMARKETS_API_PASSPHRASE"]
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
        req.body = json_body
      end

      time_finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      elapsed_time = (time_finish - time_start).round(6)

      parsed_response_body = JSON.parse(request_response.body)
      hash_method_response[:status] = 'success'
      hash_method_response[:body] = parsed_response_body
      hash_method_response[:elapsed_time] = elapsed_time

    rescue Faraday::ClientError => e
      puts "Faraday Client Error: #{e.response[:status]} #{e.response[:body]}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.response[:status]
      hash_method_response[:body] = JSON.parse(e.response[:body]) rescue e.response[:body]
    rescue => e
      puts "Unexpected error: #{e.message}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.message
    end

    hash_method_response
  end

  def close_all_futures_trades
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/futures/all/close'
      params = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'DELETE' + path + params)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.delete(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      puts e
      hash_method_response[:status] = 'error'
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

  def close_futures_trade(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/futures'
      hash_params = { id: trade_id }
      data = URI.encode_www_form(hash_params)

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'DELETE' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.delete(path, hash_params) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def close_futures_trade(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/futures'
      hash_params = { id: trade_id }
      data = URI.encode_www_form(hash_params)

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'DELETE' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.delete(path, hash_params) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
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

  def cancel_futures_trade(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = (Time.now.to_f * 1000).to_i.to_s
      path = '/futures/cancel'
      hash_params = { id: trade_id }
      string_json = hash_params.to_json

      digest = OpenSSL::Digest.new('sha256')
      payload = timestamp + 'POST' + '/v2' + path + string_json
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], payload)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.post("/v2#{path}") do |req|
        req.headers['Content-Type'] = 'application/json'
        req.headers['LNM-ACCESS-KEY'] = ENV["LNMARKETS_API_KEY"]
        req.headers['LNM-ACCESS-PASSPHRASE'] = ENV["LNMARKETS_API_PASSPHRASE"]
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
        req.body = string_json
      end

      time_finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      elapsed_time = (time_finish - time_start).round(6)

      if request_response.status == 400
        puts "400 Bad Request Error"
        puts "Response body: #{request_response.body}"
        hash_method_response[:status] = 'error'
        hash_method_response[:message] = 'Bad Request'
        hash_method_response[:body] = JSON.parse(request_response.body) rescue request_response.body
      else
        hash_method_response[:status] = 'success'
        hash_method_response[:body] = JSON.parse(request_response.body)
      end

      hash_method_response[:elapsed_time] = elapsed_time
      
    rescue Faraday::ClientError => e
      puts "Faraday Client Error: #{e.response[:status]} #{e.response[:body]}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.response[:status]
      hash_method_response[:body] = e.response[:body]
    rescue => e
      puts "Unexpected error: #{e.message}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.message
    end

    hash_method_response
  end

  def update_futures_trade(id, trade_type, value)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = (Time.now.to_f * 1000).to_i.to_s
      path = '/futures'
      hash_params = { 
        id: id, 
        type: trade_type, 
        value: value, 
      }
      json_body = hash_params.to_json

      digest = OpenSSL::Digest.new('sha256')
      payload = timestamp + 'PUT' + '/v2' + path + json_body
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], payload)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.put("/v2#{path}") do |req|
        req.headers['Content-Type'] = 'application/json'
        req.headers['LNM-ACCESS-KEY'] = ENV["LNMARKETS_API_KEY"]
        req.headers['LNM-ACCESS-PASSPHRASE'] = ENV["LNMARKETS_API_PASSPHRASE"]
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
        req.body = json_body
      end

      time_finish = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      elapsed_time = (time_finish - time_start).round(6)

      parsed_response_body = JSON.parse(request_response.body)
      hash_method_response[:status] = 'success'
      hash_method_response[:body] = parsed_response_body
      hash_method_response[:elapsed_time] = elapsed_time

    rescue Faraday::ClientError => e
      puts "Faraday Client Error: #{e.response[:status]} #{e.response[:body]}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.response[:status]
      hash_method_response[:body] = JSON.parse(e.response[:body]) rescue e.response[:body]
    rescue => e
      puts "Unexpected error: #{e.message}"
      hash_method_response[:status] = 'error'
      hash_method_response[:message] = e.message
    end

    hash_method_response
  end

  def get_futures_trade(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = "/v2/futures/trades/#{trade_id}"
      data = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + data )
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      hash_method_response[:status] = 'error'
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

  
  def get_price_btcusd_ticker()
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = "/v2/futures/ticker"
      data = ''

      lnm_signature = ''
      digest = OpenSSL::Digest.new('sha256')
      hmac = OpenSSL::HMAC.digest(digest, ENV["LNMARKETS_API_SECRET"], timestamp + 'GET' + path + data)
      lnm_signature = Base64.strict_encode64(hmac)

      request_response = @conn.get(path) do |req|
        req.headers['LNM-ACCESS-SIGNATURE'] = lnm_signature
        req.headers['LNM-ACCESS-TIMESTAMP'] = timestamp
      end
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
      puts "LnmarketsAPI Error!"
      hash_method_response[:status] = 'error'
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