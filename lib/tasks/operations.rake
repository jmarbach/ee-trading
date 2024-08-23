namespace :operations do
  task update_missing_market_data: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    #
    # Rewrites data from the last MarketDataLog entry using data from the last complete calendar day
    #
    timestamp_current = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    timestamp_yesterday = timestamp_current - 86400000
    Rails.logger.info(
      {
        message: "Run operations:update_missing_market_data...",
        body: "QUERY DATE: #{DateTime.now.utc.beginning_of_day} - #{timestamp_current}",
        script: "operations:update_missing_market_data"
      }.to_json
    )

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
    start_date = (timestamp_current - (11 * 86400000))
    end_date = timestamp_current

    rsi_values, volume_values, simple_moving_average_values, exponential_moving_average_values, macd_values, avg_funding_rate, aggregate_open_interest =
      nil, nil, nil, nil, nil, nil, nil
    data_errors = 0

    # Get technical indicators from Polygon
    response_rsi = polygon_client.get_rsi(symbol, timestamp_current, timespan, window, series_type)
    rsi_value = 0.0
    if response_rsi[:status] == 'success'
      rsi_values = response_rsi[:body]['results']['values']
      rsi_value = rsi_values[0]['value']
    else
      data_errors += 1
    end
    puts "RSI VALUES:"
    puts rsi_values
    puts ""

    response_volume = polygon_client.get_aggregate_bars(symbol, aggregates_timespan, aggregates_multiplier, start_date, end_date)
    current_day_volume, prior_day_volume, two_days_ago_volume, three_days_ago_volume, four_days_ago_volume, five_days_ago_volume, 
    six_days_ago_volume, seven_days_ago_volume = 0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    if response_volume[:status] == 'success'
      volume_values = response_volume[:body]['results']

      seven_days_ago_volume = volume_values[7]['v'].to_f
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

    response_simple_moving_average = polygon_client.get_sma(symbol, timestamp_current, timespan, window, series_type)
    simple_moving_average = 0.0
    if response_simple_moving_average[:status] == 'success'
      simple_moving_average_values = response_simple_moving_average[:body]['results']['values']
      simple_moving_average = simple_moving_average_values[0]['value']
    else
      data_errors += 1
    end
    puts "SIMPLE MOVING AVERAGE VALUES:"
    puts simple_moving_average_values
    puts ""

    response_exponential_moving_average = polygon_client.get_ema(symbol, timestamp_current, timespan, window, series_type)
    exponential_moving_average = 0.0
    if response_exponential_moving_average[:status] == 'success'
      exponential_moving_average_values = response_exponential_moving_average[:body]['results']['values']
      exponential_moving_average = exponential_moving_average_values[0]['value']
    else
      data_errors += 1
    end
    puts "EXPONENTIAL MOVING AVERAGE VALUES:"
    puts exponential_moving_average_values
    puts ""

    response_macd = polygon_client.get_macd(symbol, timestamp_current, timespan, short_window, long_window, signal_window, series_type)
    macd_value, macd_signal, macd_histogram = 0.0,0.0,0.0
    if response_macd[:status] == 'success'
      macd_values = response_macd[:body]['results']['values']
      macd_value = macd_values[0]['value']
      macd_signal = macd_values[0]['signal']
      macd_histogram = macd_values[0]['histogram']
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
      data_errors += 1
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
      data_errors += 1
    end
    puts "OPEN INTEREST:"
    puts aggregate_open_interest
    puts ""

    #
    # Find average open interest over last 8 days
    #
    last_8_market_data_log_entries = nil
    last_8_market_data_log_entries = MarketDataLog.order(recorded_date: :desc).limit(8).pluck(:aggregate_open_interest)
    if last_8_market_data_log_entries != nil
      # Remove nil values from array
      last_8_market_data_log_entries.compact!
      if !last_8_market_data_log_entries.empty?
        last_8_aggregate_open_interests_average = last_8_market_data_log_entries.sum.fdiv(last_8_market_data_log_entries.size).round(2)
      else
        last_8_aggregate_open_interests_average = 0.0
      end
    else
      last_8_aggregate_open_interests_average = 0.0
      data_errors += 1
    end

    # Last 10 1D Candle Close Avg
    last_10_candle_closes_average = 0.0
    start_date = (timestamp_current - (9 * 86400000))
    response_last_10_1d_candles = polygon_client.get_aggregate_bars(symbol, aggregates_timespan, aggregates_multiplier, start_date, end_date)
    if response_exponential_moving_average[:status] == 'success'
      last_10_1d_candles = response_last_10_1d_candles[:body]['results']

      if last_10_1d_candles.count > 0
        last_10_candle_closes_sum = 0.0
        last_10_1d_candles.each do |f|
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
    Rails.logger.info(
      {
        message: "Fetched Last BTCUSD Tick.",
        body: "#{btcusd}",
        script: "operations:update_missing_market_data"
      }.to_json
    )

    # Implied volatility deribit
    implied_volatility_deribit = 0.0
    lnmarkets_client = LnmarketsAPI.new
    lnmarkets_response = lnmarkets_client.get_options_volatility_index()
    if lnmarkets_response[:status] == 'success'
      implied_volatility_deribit = lnmarkets_response[:body]['volatilityIndex']
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get implied volatility from LnMarkets.",
          script: "operations:update_missing_market_data"
        }.to_json
      )
      data_errors += 1
    end

    # Implied volatility t3
    implied_volatility_t3 = 0.0
    t3_client = T3IndexAPI.new
    current_tick = DateTime.now.utc.strftime("%Y-%m-%d-00-00-00")
    t3_response = t3_client.get_tick(current_tick)
    if t3_response[:status] == 'success'
      implied_volatility_t3 = t3_response[:body]['value']
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get implied volatility from T3IndexAPI.",
          script: "operations:update_missing_market_data"
        }.to_json
      )
      data_errors += 1
    end

    #
    # Find average implied volatilities from T3
    #
    last_16_market_data_log_entries = nil
    last_16_market_data_log_entries = MarketDataLog.order(recorded_date: :desc).limit(16).pluck(:implied_volatility_t3)
    if last_16_market_data_log_entries != nil
      # Remote nil values from array
      last_16_market_data_log_entries.compact!
      if !last_16_market_data_log_entries.empty?
        last_16_implied_volatilities_t3_average = last_16_market_data_log_entries.sum.fdiv(last_16_market_data_log_entries.size).round(2)
      else
        last_16_implied_volatilities_t3_average = 0.0
      end
    else
      last_16_implied_volatilities_t3_average = 0.0
      data_errors += 1
    end

    sleep(62)
    # Long/Short data
    avg_long_short_ratio = 0.0
    begin
      coinalyze_client = CoinalyzeAPI.new
      start_time = (DateTime.now.utc.beginning_of_day.to_i - 86400)
      end_time = DateTime.now.utc.beginning_of_day.to_i
      symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6,BTC-PERP.V,BTC_USDT.Y,BTC_USDC-PERPETUAL.2,BTCUSDC_PERP.A,BTCUSDT_PERP.F,BTC-USD.8,BTC_USD.Y,BTC-PERPETUAL.2,BTCUSDT_PERP.3,BTCEURT_PERP.F,BTCUSDU24.6,BTCUSDZ24.6'
      coinalyze_response = coinalyze_client.get_long_short_ratio_history(symbols, 'daily', start_time, end_time)

      if coinalyze_response[:body].count > 0
        count_of_records = 0.0
        coinalyze_response[:body].each_with_index do |f, index|
          coinalyze_response[:body][index]['history'].each do |o|
            count_of_records += 1.0
            avg_long_short_ratio += o['r']
          end
        end
        avg_long_short_ratio = (avg_long_short_ratio / count_of_records).round(3)
      end
    rescue => e
      puts e
      puts 'Error fetching long/short ratio data'
      data_errors += 1
    end
    puts "LONG/SHORT RATIO:"
    puts avg_long_short_ratio
    puts ""

    #
    # Find average long/short ratios
    #
    last_30_market_data_log_entries = nil
    last_30_market_data_log_entries = MarketDataLog.order(recorded_date: :desc).limit(30).pluck(:avg_long_short_ratio)
    if last_30_market_data_log_entries != nil
      # Remote nil values from array
      last_30_market_data_log_entries.compact!
      if !last_30_market_data_log_entries.empty?
        last_30_long_short_ratios_average = last_30_market_data_log_entries.sum.fdiv(last_30_market_data_log_entries.size).round(2)
      else
        last_30_long_short_ratios_average = 0.0
      end
    else
      last_30_long_short_ratios_average = 0.0
      data_errors += 1
    end

    #
    # Save MarketDataLog
    #
    begin
      market_data_log = MarketDataLog.last.update(
        recorded_date: DateTime.now,
        price_btcusd: btcusd,
        prior_day_volume: prior_day_volume,
        two_days_ago_volume: two_days_ago_volume,
        three_days_ago_volume: three_days_ago_volume,
        four_days_ago_volume: four_days_ago_volume,
        five_days_ago_volume: five_days_ago_volume,
        six_days_ago_volume: six_days_ago_volume,
        seven_days_ago_volume: seven_days_ago_volume,
        rsi: rsi_value,
        simple_moving_average: simple_moving_average,
        exponential_moving_average: exponential_moving_average,
        macd_value: macd_value,
        macd_signal: macd_signal,
        macd_histogram: macd_histogram,
        avg_funding_rate: avg_funding_rate,
        aggregate_open_interest: aggregate_open_interest,
        avg_last_10_candle_closes: last_10_candle_closes_average,
        avg_last_8_aggregate_open_interests: last_8_aggregate_open_interests_average,
        implied_volatility_deribit: implied_volatility_deribit,
        implied_volatility_t3: implied_volatility_t3,
        avg_long_short_ratio: avg_long_short_ratio
      )
    rescue => e
      Rails.logger.error(
        {
          message: "Error. Unable to save market_data_log record.",
          body: "#{e}",
          script: "operations:update_missing_market_data"
        }.to_json
      )
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now
      )
    end
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end