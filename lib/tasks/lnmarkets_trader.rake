namespace :lnmarkets_trader do
  task check_market_indicators: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:check_market_indicators...'
    puts ''
    puts 'QUERY DATE:'
    puts DateTime.now.utc.beginning_of_day
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

    rsi_values, volume_values, simple_moving_average_values, exponential_moving_average_values, macd_values, avg_funding_rate, aggregate_open_interest =
      nil, nil, nil, nil, nil, nil, nil
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
    puts "FUNDING RATE:"
    puts avg_funding_rate
    puts ""

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
    puts "OPEN INTEREST:"
    puts aggregate_open_interest
    puts ""

    # Last 10 1D Candle Close Avg
    last_10_candle_closes_average = 0.0
    start_date = (timestamp - (9 * 86400000))
    response_last_10_1d_candles = polygon_client.get_aggregate_bars(symbol, aggregates_timespan, aggregates_multiplier, start_date, end_date)
    if response_exponential_moving_average[:status] == 'success'
      last_10_1d_candles = response_last_10_1d_candles[:body]['results']

      if last_10_1d_candles.count > 0
        last_10_candle_closes_sum = 0.0
        last_10_1d_candles.each_with_index do |f, index|
          last_10_candle_closes_sum += f['c']
        end
        last_10_candle_closes_average = (last_10_candle_closes_sum/last_10_1d_candles.count).round(4)
      end
    else
      data_errors += 1
    end
    puts "LAST 10 1D CANDLES CLOSE AVG:"
    puts last_10_candle_closes_average
    puts ""

    # Current BTCUSD price
    btcusd = 0.0
    currency_from = 'BTC'
    currency_to = 'USD'
    response_btcusd = polygon_client.get_last_trade(currency_from, currency_to)
    if response_btcusd[:status] == 'success'
      btcusd = response_btcusd[:body]['last']['price']
    else
      data_errors += 1
    end
    puts "LAST BTCUSD TICK:"
    puts btcusd
    puts ""

    #
    # Initialize score
    #
    trade_direction_score = 0.0

    #
    # Evaluate rules
    #
    if volume_values.present?
      if (prior_day_volume < two_days_ago_volume &&
        prior_day_volume < three_days_ago_volume &&
        prior_day_volume < four_days_ago_volume &&
        prior_day_volume < five_days_ago_volume &&
        prior_day_volume < six_days_ago_volume) || (
        (two_days_ago_volume < three_days_ago_volume &&
        two_days_ago_volume < four_days_ago_volume &&
        two_days_ago_volume < five_days_ago_volume &&
        two_days_ago_volume < six_days_ago_volume) && (prior_day_volume < three_days_ago_volume))
        trade_direction_score -= 1.0
      end

      if (prior_day_volume > two_days_ago_volume &&
        two_days_ago_volume > three_days_ago_volume &&
        three_days_ago_volume > four_days_ago_volume &&
        four_days_ago_volume > five_days_ago_volume)
        trade_direction_score += 2.0
      end
    else
      data_errors += 1
    end

    if rsi_values.present?
      if rsi_values[0]['value'] > 65 && rsi_values[0]['value'] < 100
        trade_direction_score += 3.0
      else
        trade_direction_score -= 1.0
      end
    else
      data_errors += 1
    end

    if macd_values.present?
      if macd_values[0]['histogram'] < 100
        trade_direction_score += 1.0
      elsif macd_values[0]['histogram'] > 500.0
        trade_direction_score -= 2.0
      end
    else
      data_errors += 1
    end

    if exponential_moving_average_values.present? && simple_moving_average_values.present?
      if exponential_moving_average_values[0]['value'] > (simple_moving_average_values[0]['value'] * 1.019)
        trade_direction_score += 1.0
      elsif exponential_moving_average_values[0]['value'] < (simple_moving_average_values[0]['value'] * 0.98)
        trade_direction_score -= 1.0
      end
    else
      data_errors += 1
    end

    if avg_funding_rate != nil
      if avg_funding_rate > 0.003 && avg_funding_rate < 0.01
        trade_direction_score -= 6.0
      elsif avg_funding_rate < -0.008 && avg_funding_rate > -0.01
        trade_direction_score += 3.0
      end
    else
      data_errors += 1
    end

    if last_10_candle_closes_average != 0.0
      if btcusd > ((last_10_candle_closes_average) * 1.20)
        trade_direction_score -= 6.0
      end
    else
      data_errors += 1
    end

    # if last_10_aggregate_open_interests.size == 8 && aggregate_open_interest != 0.0
    #   if aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.015) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.02)
    #     trade_direction_score -= 1.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.02) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.04)
    #     trade_direction_score -= 1.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.04) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.10)
    #     trade_direction_score += 0.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.10) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.11)
    #     trade_direction_score -= 1.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.11) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.12)
    #     trade_direction_score += 1.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.14) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.20)
    #     trade_direction_score += 1.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.20) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.30)
    #     trade_direction_score += 2.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.30) && aggregate_open_interest < ((last_10_aggregate_open_interests_average)*1.40)
    #     trade_direction_score -= 5.0
    #   elsif aggregate_open_interest > ((last_10_aggregate_open_interests_average)*1.40)
    #     trade_direction_score += 1.0
    #   end

    #   if aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.98) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.960)
    #     trade_direction_score -= 0.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.96) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.95)
    #     trade_direction_score += 1.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.95) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.94)
    #     trade_direction_score -= 5.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.92) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.88)
    #     trade_direction_score -= 3.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.88) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.80)
    #     trade_direction_score -= 3.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.80) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.70)
    #     trade_direction_score += 1.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.70) && aggregate_open_interest > ((last_10_aggregate_open_interests_average)*0.60)
    #     trade_direction_score += 1.0
    #   elsif aggregate_open_interest < ((last_10_aggregate_open_interests_average)*0.60)
    #     trade_direction_score += 1.0
    #   end
    # end
    puts "trade_direction_score: #{trade_direction_score}"

    puts 'End lnmarkets_trader:check_market_indicators...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end
