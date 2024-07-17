namespace :lnmarkets_trader do
  task check_market_indicators: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:check_market_indicators...'
    puts 'Query date:'
    puts DateTime.now
    timestamp = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    puts timestamp

    # Standard model inputs
    polygon_client = PolygonAPI.new
    symbol = 'X:BTCUSD'
    timespan = 'hour'
    window = 24
    short_window = 48
    long_window = 104
    signal_window = 36
    series_type = 'close'

    # Extra params for aggregates
    aggregates_timespan = 'day'
    aggregates_multiplier = 1

    # Extra params for aggregates
    start_date = (timestamp - (11 * 86400000))
    end_date = timestamp

    rsi_values, volume_values, simple_moving_average_values, exponential_moving_average_values, macd_values, avg_funding_rate =
      nil, nil, nil, nil, nil, nil
    data_errors = 0

    # Get technical indicators from Polygon
    response_rsi = polygon_client.get_rsi(symbol, timestamp, timespan, window, series_type)
    if response_rsi[:status] == 'success'
      rsi_values = response_rsi[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "RSI VALUES:"
    puts rsi_values
    puts ""

    response_volume = polygon_client.get_aggregate_bars(symbol, aggregates_timespan, aggregates_multiplier, start_date, end_date)
    if response_volume[:status] == 'success'
      volume_values = response_volume[:body]['results']

      six_days_ago_volume = volume_values[6]['v'].to_f
      five_days_ago_volume = volume_values[5]['v'].to_f
      four_days_ago_volume = volume_values[4]['v'].to_f
      three_days_ago_volume = volume_values[3]['v'].to_f
      two_days_ago_volume = volume_values[2]['v'].to_f
      prior_day_volume = volume_values[1]['v'].to_f
      current_day_volume = volume_values[0]['v'].to_f
    else
      data_errors += 1
    end
    puts "VOLUME VALUES:"
    puts volume_values
    puts ""

    response_simple_moving_average = polygon_client.get_sma(symbol, timestamp, timespan, window, series_type)
    if response_simple_moving_average[:status] == 'success'
      simple_moving_average_values = response_simple_moving_average[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "SIMPLE MOVING AVERAGE VALUES:"
    puts simple_moving_average_values
    puts ""

    response_exponential_moving_average = polygon_client.get_ema(symbol, timestamp, timespan, window, series_type)
    if response_exponential_moving_average[:status] == 'success'
      exponential_moving_average_values = response_exponential_moving_average[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "EXPONENTIAL MOVING AVERAGE VALUES:"
    puts exponential_moving_average_values
    puts ""

    response_macd = polygon_client.get_macd(symbol, timestamp, timespan, short_window, long_window, signal_window, series_type)
    if response_macd[:status] == 'success'
      macd_values = response_macd[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "MACD:"
    puts macd_values
    puts ""

    # Funding data
    avg_funding_rate = 0.0
    begin
      coinalyze_client = CoinalyzeAPI.new
      start_time = (DateTime.now.utc.beginning_of_day.to_i - 86400)
      end_time = DateTime.now.utc.beginning_of_day.to_i
      symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6'
      coinalyze_response = coinalyze_client.get_avg_funding_history(symbols, 'daily', start_time, end_time)

      if coinalyze_response[:body].count > 0
        coinalyze_response[:body].each_with_index do |f, index|
          avg_funding_rate += coinalyze_response[:body][index]['history'][0]['c']
        end
        avg_funding_rate = (avg_funding_rate/coinalyze_response[:body].count).round(4)
      end
    rescue => e
      puts e
      puts 'Error fetching funding rate data'
    end

    # Open Interest data
    aggregate_open_interest = 0.0
    begin
      coinalyze_client = CoinalyzeAPI.new
      start_time = (DateTime.now.utc.beginning_of_day.to_i - 86400)
      end_time = DateTime.now.utc.beginning_of_day.to_i
      symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6,BTC-PERP.V,BTC_USDT.Y,BTC_USDC-PERPETUAL.2,BTCUSDC_PERP.A,BTCUSDT_PERP.F,BTC-USD.8,BTC_USD.Y,BTC-PERPETUAL.2,BTCUSDT_PERP.3,BTCEURT_PERP.F,BTCUSDU24.6,BTCUSDZ24.6'
      coinalyze_response = coinalyze_client.get_avg_open_interest_history(symbols, 'daily', start_time, end_time)

      if coinalyze_response[:body].count > 0
        coinalyze_response[:body].each_with_index do |f, index|
          aggregate_open_interest += coinalyze_response[:body][index]['history'][0]['c']
        end
        aggregate_open_interest = aggregate_open_interest.round(1)
      end
    rescue => e
      puts e
      puts 'Error fetching open interest data'
    end

    # Make trade decision
    trade_direction_score = 0.0

    puts "trade_direction_score: #{trade_direction_score}"

    puts 'End lnmarkets_trader:check_market_indicators...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end
