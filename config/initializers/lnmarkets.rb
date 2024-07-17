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
      request: { timeout: 10 }
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

  def close_option_contract(trade_id)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options'
      puts "trade_id: #{trade_id}"
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

  def get_options_trades(status)
    hash_method_response = { status: '', message: '', body: '', elapsed_time: '' }
    begin
      time_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      timestamp = DateTime.now.to_i.in_milliseconds.to_s
      path = '/v2/options'
      hash_params = { status: status }
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
      path = '/v2/user'
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