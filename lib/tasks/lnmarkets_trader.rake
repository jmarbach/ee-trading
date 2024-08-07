namespace :lnmarkets_trader do
  task check_market_indicators: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    timestamp_current = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    timestamp_yesterday = timestamp_current - 86400000
    Rails.logger.info(
      {
        message: "Run lnmarkets_trader:check_market_indicators...",
        body: "QUERY DATE: #{DateTime.now.utc.beginning_of_day} - #{timestamp_current}",
        script: "lnmarkets_trader:check_market_indicators"
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
    if response_simple_moving_average[:status] == 'success'
      simple_moving_average_values = response_simple_moving_average[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "SIMPLE MOVING AVERAGE VALUES:"
    puts simple_moving_average_values
    puts ""

    response_exponential_moving_average = polygon_client.get_ema(symbol, timestamp_current, timespan, window, series_type)
    if response_exponential_moving_average[:status] == 'success'
      exponential_moving_average_values = response_exponential_moving_average[:body]['results']['values']
    else
      data_errors += 1
    end
    puts "EXPONENTIAL MOVING AVERAGE VALUES:"
    puts exponential_moving_average_values
    puts ""

    response_macd = polygon_client.get_macd(symbol, timestamp_current, timespan, short_window, long_window, signal_window, series_type)
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
        script: "lnmarkets_trader:check_market_indicators"
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
          script: "lnmarkets_trader:check_market_indicators"
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
          script: "lnmarkets_trader:check_market_indicators"
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
      end
    else
      last_16_implied_volatilities_t3_average = 0.0
      data_errors += 1
    end

    #
    # Save MarketDataLog
    #
    begin
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        price_btcusd: btcusd,
        prior_day_volume: prior_day_volume,
        two_days_ago_volume: two_days_ago_volume,
        three_days_ago_volume: three_days_ago_volume,
        four_days_ago_volume: four_days_ago_volume,
        five_days_ago_volume: five_days_ago_volume,
        six_days_ago_volume: six_days_ago_volume,
        seven_days_ago_volume: seven_days_ago_volume,
        rsi: rsi_values[0]['value'],
        simple_moving_average: simple_moving_average_values[0]['value'],
        exponential_moving_average: exponential_moving_average_values[0]['value'],
        macd_value: macd_values[0]['value'],
        macd_signal: macd_values[0]['signal'],
        macd_histogram: macd_values[0]['histogram'],
        avg_funding_rate: avg_funding_rate,
        aggregate_open_interest: aggregate_open_interest,
        avg_last_10_candle_closes: last_10_candle_closes_average,
        avg_last_8_aggregate_open_interests: last_8_aggregate_open_interests_average,
        implied_volatility_deribit: implied_volatility_deribit,
        implied_volatility_t3: implied_volatility_t3
      )
    rescue => e
      Rails.logger.error(
        {
          message: "Error. Unable to save market_data_log record.",
          body: "#{e}",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now
      )
    end

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

    if last_8_aggregate_open_interests_average != 0.0 && aggregate_open_interest != 0.0
      if aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.015) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.02)
        trade_direction_score -= 1.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.02) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.04)
        trade_direction_score -= 1.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.04) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.10)
        trade_direction_score += 0.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.10) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.11)
        trade_direction_score -= 1.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.11) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.12)
        trade_direction_score += 1.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.14) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.20)
        trade_direction_score += 1.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.20) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.30)
        trade_direction_score += 2.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.30) && aggregate_open_interest < ((last_8_aggregate_open_interests_average)*1.40)
        trade_direction_score -= 5.0
      elsif aggregate_open_interest > ((last_8_aggregate_open_interests_average)*1.40)
        trade_direction_score += 1.0
      end

      if aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.98) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.960)
        trade_direction_score -= 0.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.96) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.95)
        trade_direction_score += 1.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.95) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.94)
        trade_direction_score -= 5.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.92) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.88)
        trade_direction_score -= 3.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.88) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.80)
        trade_direction_score -= 3.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.80) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.70)
        trade_direction_score += 1.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.70) && aggregate_open_interest > ((last_8_aggregate_open_interests_average)*0.60)
        trade_direction_score += 1.0
      elsif aggregate_open_interest < ((last_8_aggregate_open_interests_average)*0.60)
        trade_direction_score += 1.0
      end
    end

    if last_16_implied_volatilities_t3_average != 0.0 && implied_volatility_t3 != 0.0
      if implied_volatility_t3 > ((last_16_implied_volatilities_t3_average)*1.31) && implied_volatility_t3 < ((last_16_implied_volatilities_t3_average)*1.40)
        trade_direction_score -= 1.0
      elsif implied_volatility_t3 > ((last_16_implied_volatilities_t3_average)*1.49) && implied_volatility_t3 < ((last_16_implied_volatilities_t3_average)*1.79)
        trade_direction_score -= 1.0
      elsif implied_volatility_t3 < ((last_16_implied_volatilities_t3_average)*0.97) && implied_volatility_t3 > ((last_16_implied_volatilities_t3_average)*0.92)
        trade_direction_score -= 1.0
      elsif implied_volatility_t3 < ((last_16_implied_volatilities_t3_average)*0.89) && implied_volatility_t3 > ((last_16_implied_volatilities_t3_average)*0.75)
        trade_direction_score -= 2.0
      end
    end

    #
    # Save ScoreLog
    #
    Rails.logger.info(
      {
        message: "Final Trade Direction Score: #{trade_direction_score}",
        script: "lnmarkets_trader:check_market_indicators"
      }.to_json
    )
    begin
      score_log = ScoreLog.create(
        recorded_date: DateTime.now,
        market_data_log_id: market_data_log.id,
        score: trade_direction_score
      )
    rescue => e
      Rails.logger.error(
        {
          message: "Error saving score_log record",
          body: e,
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

    #
    # Close existing futures and options contracts
    #
    puts ""
    puts "1. Check existing running options contracts..."
    puts "--------------------------------------------"
    running_contracts = []
    lnmarkets_response = lnmarkets_client.get_options_trades()
    if lnmarkets_response[:status] == 'success'
      all_contracts = lnmarkets_response[:body]
      if all_contracts.count > 0
        running_contracts = all_contracts.select { |a| a['running'] == true }
        Rails.logger.info(
          {
            message: "Running Options Contracts: #{running_contracts.count}",
            script: "lnmarkets_trader:check_market_indicators"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get running contracts.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    if running_contracts.any?
      puts ""
      puts "Close all open options contracts from prior trading interval..."
      puts ""
      #
      # Close all 'running' contracts
      #
      running_contracts.each do |c|
        lnmarkets_response = lnmarkets_client.close_options_contract(c['id'])

        if lnmarkets_response[:status] == 'success'
          puts ""
          puts "Finished closing open options contract: #{c['id']}."
          puts ""
          #
          # Update Trade Log
          #
          trade_log = TradeLog.find_by_external_id(c['id'])
          if trade_log.present?
            trade_log.update(
              running: false,
              closed: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.error(
            {
              message: "Error. Unable to find trade log for trade: #{c['id']}",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close open options contracts: #{c['id']}",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No running contracts.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    puts ""
    puts "2. Check existing closed, open, and running futures trades..."
    puts "--------------------------------------------"
    closed_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('closed', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      closed_futures = lnmarkets_response[:body]
      if closed_futures.count > 0
        Rails.logger.info(
          {
            message: "Closed Futures: #{closed_futures.count}",
            script: "lnmarkets_trader:check_market_indicators"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get closed futures trades.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    if closed_futures.any?
      Rails.logger.info(
        {
          message: "Get trade stats from futures closed in prior trading interval...",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      #
      # Cancel all open futures trades
      #
      closed_futures.each do |f|
        #
        # Update Trade Log
        #
        trade_log = TradeLog.find_by_external_id(f['id'])
        if trade_log.present?
          trade_log.update(
            open: false,
            running: false,
            closed: true,
            canceled: false
          )
          trade_log.get_final_trade_stats
        else
          Rails.logger.error(
            {
              message: "Error. Unable to find trade log for trade: #{f['id']}",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No closed futures trades in prior interval.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    open_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('open', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      open_futures = lnmarkets_response[:body]
      if open_futures.count > 0
        Rails.logger.info(
          {
            message: "Open Futures: #{open_futures.count}",
            script: "lnmarkets_trader:check_market_indicators"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get open futures trades.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    if open_futures.any?
      Rails.logger.info(
        {
          message: "Cancel all open futures trades from prior trading interval...",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      #
      # Cancel all open futures trades
      #
      open_futures.each do |f|
        lnmarkets_response = lnmarkets_client.cancel_futures_trade(f['id'])
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Finished closing open futures trade: #{f['id']}.",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
          #
          # Update Trade Log
          #
          trade_log = TradeLog.find_by_external_id(f['id'])
          if trade_log.present?
            trade_log.update(
              open: false,
              canceled: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.error(
              {
                message: "Error. Unable to find trade log for trade: #{f['id']}",
                script: "lnmarkets_trader:check_market_indicators"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close futures trade: #{f['id']}",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No open futures trades.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    running_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('running', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      running_futures = lnmarkets_response[:body]
      if running_futures.count > 0
        Rails.logger.info(
          {
            message: "Running Futures: #{running_futures.count}",
            script: "lnmarkets_trader:check_market_indicators"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get running futures trades.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    if running_futures.any?
      Rails.logger.info(
        {
          message: "Close all running futures trades from prior trading interval...",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      #
      # Close all futures trades
      #
      running_futures.each do |f|
        lnmarkets_response = lnmarkets_client.close_futures_trade(f['id'])
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Finished closing futures trade: #{f['id']}.",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
          #
          # Update Trade Log
          #
          trade_log = TradeLog.find_by_external_id(f['id'])
          if trade_log.present?
            trade_log.update(
              open: false,
              canceled: false,
              running: false,
              closed: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.error(
              {
                message: "Error. Unable to find trade log for trade: #{f['id']}",
                script: "lnmarkets_trader:check_market_indicators"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close futures trade: #{f['id']}",
              script: "lnmarkets_trader:check_market_indicators"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No running futures trades.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    puts ""
    puts "3. Save TradingStatsDaily record"
    puts "--------------------------------------------"
    puts ""
    Rake::Task["accountant:save_trading_stats_daily"].execute

    puts ""
    puts "4. Proceed to create new trade..."
    puts "--------------------------------------------"
    puts ""
    #
    # Invoke trade order scripts
    #
    if trade_direction_score < 0.0 || trade_direction_score > 0.0
      puts "********************************************"
      puts "********************************************"
      trade_direction = ''
      if trade_direction_score > 0.0
        trade_direction = 'buy'
      elsif trade_direction_score < 0.0
        trade_direction = 'sell'
      end
      Rails.logger.info(
        {
          message: "Proceed with new trade direction.",
          body: "#{trade_direction}",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )

      if trade_direction == 'buy'
        Rake::Task["lnmarkets_traders:create_long_trade"].execute({score_log_id: score_log.id})
      elsif trade_direction == 'sell'
        Rake::Task["lnmarkets_trader:create_short_trade"].execute({score_log_id: score_log.id})
      end
      Rails.logger.info(
        {
          message: "Finished creating new #{trade_direction} trade.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
      puts "********************************************"
      puts "********************************************"
    elsif trade_direction_score == 0.0
      #
      # No trade... wait for new market indicators at next trading interval
      #
      Rails.logger.info(
        {
          message: "No trade.",
          script: "lnmarkets_trader:check_market_indicators"
        }.to_json
      )
    end

    Rails.logger.info(
      {
        message: "Data Errors: #{data_errors}",
        script: "lnmarkets_trader:check_market_indicators"
      }.to_json
    )
    if data_errors > 0
      market_data_log.update(int_data_errors: data_errors)
    end

    puts 'End lnmarkets_trader:check_market_indicators...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task :create_long_trade, [:score_log_id] => :environment do |t, args|
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:create_long_trade...'
    puts ''
    Rails.logger.info(
      {
        message: "args[:score_log_id]: #{args[:score_log_id]}",
        script: "lnmarkets_trader:create_long_trade"
      }.to_json
    )
    if !args[:score_log_id].present?
      Rails.logger.fatal(
        {
          message: "Error. Invocation missing required params.",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )
      abort 'Unable to invoke create_long_trade script.'
    else
      score_log_id = args[:score_log_id]
    end
    # Initialize lnmarkets_client
    lnmarkets_client = LnmarketsAPI.new

    #
    # 1. Get current account state
    #
    lnmarkets_response = lnmarkets_client.get_user_info
    if lnmarkets_response[:status] == 'success'
      #
      # Establish balance available to trade
      #
      sats_balance = lnmarkets_response[:body]['balance'].to_f.round(2)
      Rails.logger.info(
        {
          message: "Sats Available: #{sats_balance.to_fs(:delimited)}",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )

      #
      # Fetch latest price of BTCUSD
      #
      index_price_btcusd, ask_price_btcusd, bid_price_btcusd = 0.0, 0.0, 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        ask_price_btcusd = lnmarkets_response[:body]['askPrice']
        bid_price_btcusd = lnmarkets_response[:body]['bidPrice']
        Rails.logger.info(
          {
            message: "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}",
            script: "lnmarkets_trader:create_long_trade"
          }.to_json
        )

        price_sat_usd = (index_price_btcusd/100000000.0).round(5)
        balance_usd = (sats_balance * price_sat_usd).round(2)
        Rails.logger.info(
          {
            message: "Balance USD: #{balance_usd.to_fs(:delimited)}",
            script: "lnmarkets_trader:create_long_trade"
          }.to_json
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch latest price for BTCUSD... abort create_long_trade script.",
            script: "lnmarkets_trader:create_long_trade"
          }.to_json
        )
        abort 'Unable to proceed with creating a long trade without BTCUSD price.'
      end

      #
      # Define leverage factor
      #
      last_16_market_data_log_entries = MarketDataLog.order(recorded_date: :desc).limit(16).pluck(:implied_volatility_t3)
      if last_16_market_data_log_entries != nil
        # Remove nil values from array
        last_16_market_data_log_entries.compact!
        if !last_16_market_data_log_entries.empty?
          last_16_implied_volatilities_t3_average = last_16_market_data_log_entries.sum.fdiv(last_16_market_data_log_entries.size).round(2)
        end
      else
        last_16_implied_volatilities_t3_average = 0.0
      end

      if (last_16_market_data_log_entries[0] < (last_16_implied_volatilities_t3_average))
        leverage_factor = 2.69
      else
        leverage_factor = 2.55
      end
      Rails.logger.info(
        {
          message: "Leverage: #{leverage_factor}",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )

      #
      # Determine capital waged
      #
      capital_waged_usd = (balance_usd * leverage_factor).round(2)
      Rails.logger.info(
        {
          message: "Capital Waged with Leverage: #{capital_waged_usd.to_fs(:delimited)}",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )

      #
      # Execute Buy Limit order
      #
      #
      side = 'b'
      type = 'l'
      leverage = 3
      price = (bid_price_btcusd * 0.999925).round(0)
      quantity = capital_waged_usd
      takeprofit = (index_price_btcusd * 1.07).round(0)
      stoploss = (index_price_btcusd * 0.94).round(0)

      lnmarkets_response = lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "New Futures Trade Created",
            body: lnmarkets_response[:body],
            script: "lnmarkets_trader:create_long_trade"
          }.to_json
        )
        #
        # Create new record in TradeLogs table
        #
        quantity_btc_sats = ((lnmarkets_response[:body]['quantity']/index_price_btcusd)*100000000.0).round(0)
        margin_quantity_usd_cents = ((price_sat_usd * lnmarkets_response[:body]['margin']).round(0) * 100.0).round(0)
        margin_percent_of_quantity = (lnmarkets_response[:body]['margin'].to_f/quantity_btc_sats.to_f).round(4)

        trade_log = TradeLog.create(
          score_log_id: score_log_id,
          external_id: lnmarkets_response[:body]['id'],
          exchange_name: 'lnmarkets',
          derivative_type: 'futures',
          trade_type: 'buy',
          trade_direction: 'long',
          quantity_usd_cents: (lnmarkets_response[:body]['quantity'] * 100.0),
          quantity_btc_sats: quantity_btc_sats,
          open_fee: lnmarkets_response[:body]['opening_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          margin_quantity_btc_sats: lnmarkets_response[:body]['margin'],
          margin_quantity_usd_cents: margin_quantity_usd_cents,
          leverage_quantity: lnmarkets_response[:body]['leverage'],
          open_price: lnmarkets_response[:body]['price'],
          creation_timestamp: lnmarkets_response[:body]['creation_ts'],
          last_update_timestamp: lnmarkets_response[:body]['last_update_ts'],
          margin_percent_of_quantity: margin_percent_of_quantity
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'short', amount: quantity, score_log_id: score_log_id})
      else
        puts 'Error. Unable to create futures trade.'
      end
    else
      puts 'Error. Unable to fetch account balance info... skip trade.'
    end
    puts ''
    puts 'End lnmarkets_trader:create_long_trade...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task :create_short_trade, [:score_log_id] => :environment do |t, args|
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:create_short_trade...'
    puts ''
    puts 
    Rails.logger.info(
      {
        message: "args[:score_log_id]: #{args[:score_log_id]}",
        script: "lnmarkets_trader:create_short_trade"
      }.to_json
    )
    if !args[:score_log_id].present?
      Rails.logger.fatal(
        {
          message: "Error. Invocation missing required params.",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )
      abort 'Unable to invoke create_short_trade script.'
    else
      score_log_id = args[:score_log_id]
    end
    # Initialize lnmarkets_client
    lnmarkets_client = LnmarketsAPI.new

    #
    # 1. Get current account state
    #
    lnmarkets_response = lnmarkets_client.get_user_info
    if lnmarkets_response[:status] == 'success'
      #
      # Establish balance available to trade
      #
      sats_balance = lnmarkets_response[:body]['balance'].to_f.round(2)
      puts "Sats Available: #{sats_balance.to_fs(:delimited)}"
      puts ""

      #
      # Fetch latest price of BTCUSD
      #
      index_price_btcusd, ask_price_btcusd, bid_price_btcusd = 0.0, 0.0, 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        ask_price_btcusd = lnmarkets_response[:body]['askPrice']
        bid_price_btcusd = lnmarkets_response[:body]['bidPrice']
        Rails.logger.info(
          {
            message: "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}",
            script: "lnmarkets_trader:create_short_trade"
          }.to_json
        )

        price_sat_usd = (index_price_btcusd/100000000.0).round(5)
        balance_usd = (sats_balance * price_sat_usd).round(2)
        Rails.logger.info(
          {
            message: "Balance USD: #{balance_usd.to_fs(:delimited)}",
            script: "lnmarkets_trader:create_short_trade"
          }.to_json
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch latest price for BTCUSD... abort create_short_trade script.",
            script: "lnmarkets_trader:create_short_trade"
          }.to_json
        )
        abort 'Unable to proceed with creating a long trade without BTCUSD price.'
      end

      #
      # Define leverage factor
      #
      last_16_market_data_log_entries = MarketDataLog.order(recorded_date: :desc).limit(16).pluck(:implied_volatility_t3)
      if last_16_market_data_log_entries != nil
        # Remove nil values from array
        last_16_market_data_log_entries.compact!
        if !last_16_market_data_log_entries.empty?
          last_16_implied_volatilities_t3_average = last_16_market_data_log_entries.sum.fdiv(last_16_market_data_log_entries.size).round(2)
        end
      else
        last_16_implied_volatilities_t3_average = 0.0
      end

      if (last_16_market_data_log_entries[0] < (last_16_implied_volatilities_t3_average))
        leverage_factor = 2.68
      else
        leverage_factor = 2.55
      end
      Rails.logger.info(
        {
          message: "Leverage: #{leverage_factor}",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )

      #
      # Determine capital waged
      #
      capital_waged_usd = (balance_usd * leverage_factor).round(2)
      Rails.logger.info(
        {
          message: "Capital Waged with Leverage: #{capital_waged_usd.to_fs(:delimited)}",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )

      #
      # Execute Short Limit order
      #
      #
      side = 's'
      type = 'l'
      leverage = 3
      price = (bid_price_btcusd * 1.000025).round(0)
      quantity = capital_waged_usd
      takeprofit = (index_price_btcusd * 0.93).round(0)
      stoploss = (index_price_btcusd * 1.06).round(0)

      lnmarkets_response = lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "New Futures Trade Created",
            body: lnmarkets_response[:body],
            script: "lnmarkets_trader:create_short_trade"
          }.to_json
        )
        #
        # Create new record in TradeLogs table
        #
        quantity_btc_sats = ((lnmarkets_response[:body]['quantity']/index_price_btcusd)*100000000.0).round(0)
        margin_quantity_usd_cents = ((price_sat_usd * lnmarkets_response[:body]['margin']).round(0) * 100.0).round(0)
        margin_percent_of_quantity = (lnmarkets_response[:body]['margin'].to_f/quantity_btc_sats.to_f).round(4)

        trade_log = TradeLog.create(
          score_log_id: args[:score_log_id],
          external_id: lnmarkets_response[:body]['id'],
          exchange_name: 'lnmarkets',
          derivative_type: 'futures',
          trade_type: 'sell',
          trade_direction: 'short',
          quantity_usd_cents: (lnmarkets_response[:body]['quantity'] * 100.0),
          quantity_btc_sats: quantity_btc_sats,
          open_fee: lnmarkets_response[:body]['opening_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          margin_quantity_btc_sats: lnmarkets_response[:body]['margin'],
          margin_quantity_usd_cents: margin_quantity_usd_cents,
          leverage_quantity: lnmarkets_response[:body]['leverage'],
          open_price: lnmarkets_response[:body]['price'],
          creation_timestamp: lnmarkets_response[:body]['creation_ts'],
          last_update_timestamp: lnmarkets_response[:body]['last_update_ts'],
          margin_percent_of_quantity: margin_percent_of_quantity
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'long', amount: quantity, score_log_id: score_log_id})
      else
        Rails.logger.error(
          {
            message: "Error. Unable to create futures trade.",
            script: "lnmarkets_trader:create_short_trade"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to fetch account balance info... skip trade.",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )
    end
    puts 'End lnmarkets_trader:create_short_trade...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task :open_options_contract, [:direction, :amount, :score_log_id] => :environment do |t, args|
    #
    # Invoke this script with the following command:
    # Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'long', amount: lnmarkets_response[:body]['quantity'], score_log_id: score_log_id})
    #
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    Rails.logger.info(
      {
        message: "Run lnmarkets_trader:open_options_contract...",
        script: "lnmarkets_trader:open_options_contract"
      }.to_json
    )
    puts "args[:direction]: #{args[:direction]}"
    puts "args[:amount]: #{args[:amount]}"
    puts "args[:score_log_id]: #{args[:score_log_id]}"
    if args[:direction].present? && 
      args[:amount].present? &&
      args[:score_log_id].present?

      if ['long', 'short'].include?(args[:direction])
        direction = args[:direction]
      else
        Rails.logger.fatal(
          {
            message: "Error. Invalid trade direction parameter.",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )
        abort 'Unable to invoke open_options_contract script.'
      end

      # Assign capital waged based on Lnmarkets trading limits
      if args[:amount] < 500000.00
        capital_waged_usd = args[:amount]
        Rails.logger.info(
          {
            message: "Var capital_waged_usd assigned.",
            body: "#{capital_waged_usd}",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )
      else
        capital_waged_usd = 499999.00
        Rails.logger.warn(
          {
            message: "args[:amount] too high for options trade. Adjusting capital_waged_usd to 499,999.00. capital_waged_usd: #{capital_waged_usd}",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )
      end
      score_log_id = args[:score_log_id]

      # Initialize lnmarkets_client
      lnmarkets_client = LnmarketsAPI.new

      #
      # 1. Get current account state
      #
      lnmarkets_response = lnmarkets_client.get_user_info
      if lnmarkets_response[:status] == 'success'
        #
        # Establish balance available to trade
        #
        sats_balance = lnmarkets_response[:body]['balance'].to_f.round(2)
        Rails.logger.info(
          {
            message: "Sats Balance Available: #{sats_balance.to_fs(:delimited)}",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )

        #
        # Fetch latest price of BTCUSD
        #
        index_price_btcusd, ask_price_btcusd, bid_price_btcusd = 0.0, 0.0, 0.0
        lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
        if lnmarkets_response[:status] == 'success'
          index_price_btcusd = lnmarkets_response[:body]['index']
          ask_price_btcusd = lnmarkets_response[:body]['askPrice']
          bid_price_btcusd = lnmarkets_response[:body]['bidPrice']
          Rails.logger.info(
            {
              message: "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )

          price_sat_usd = (index_price_btcusd/100000000.0).round(5)
          balance_usd = (sats_balance * price_sat_usd).round(2)
          Rails.logger.info(
            {
              message: "Balance USD: #{balance_usd.to_fs(:delimited)}",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
        else
          Rails.logger.fatal(
            {
              message: "Error. Unable to fetch latest price for BTCUSD... abort open_options_contract script.",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
          abort 'Unable to proceed with creating a long trade without BTCUSD price.'
        end

        #
        # Verify available balance to satisfy margin requirement
        # - About 2% of capital waged is required for In-the-Money options
        #
        approx_margin_requirement_usd = (capital_waged_usd * 0.02).round(2)
        if approx_margin_requirement_usd > balance_usd
          new_capital_waged_usd = (balance_usd/0.025).round(0)
          Rails.logger.warn(
            {
              message: "Not enough margin balance available to attempt trade. Adjust capital_waged_usd from #{capital_waged_usd} to #{new_capital_waged_usd}.",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
          capital_waged_usd = new_capital_waged_usd
        end

        #
        # 2. Fetch options instruments
        #
        lnmarkets_response = lnmarkets_client.get_options_instruments
        if lnmarkets_response[:status] == 'success'
          filtered_instruments = lnmarkets_response[:body].select {|y| y.include?((DateTime.now + 1.day).utc.strftime("BTC.%Y-%m-%d")) }

          if direction == 'long'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.C') }
            filtered_instruments = filtered_instruments.select { |y| y.include?((index_price_btcusd-1000).ceil(-3).to_s) }
          elsif direction == 'short'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.P') }
            filtered_instruments = filtered_instruments.select { |y| y.include?((index_price_btcusd).ceil(-3).to_s) }
          end
        else
          Rails.logger.fatal(
            {
              message: "Unable to fetch options instruments.",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
          abort 'Error. Unable to fetch options instruments.'
        end

        if filtered_instruments.any?
          Rails.logger.info(
            {
              message: "Found options instrument: #{filtered_instruments[0]}",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
        else
          Rails.logger.error(
            {
              message: "Unable to find suitable options instrument.",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
          abort 'No viable options instruments found.'
        end

        #
        # 3. Open options contract
        #
        side = 'b'
        quantity = capital_waged_usd
        settlement = 'cash'
        instrument_name = filtered_instruments[0]
        lnmarkets_response = lnmarkets_client.open_option_contract(side, quantity, settlement, instrument_name)
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "New Options Trade Created:",
              body: "#{lnmarkets_response[:body]}",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
          #
          # Create new record in TradeLogs table
          #
          quantity_btc_sats = ((lnmarkets_response[:body]['quantity']/index_price_btcusd)*100000000.0).round(0)
          margin_quantity_usd_cents = ((price_sat_usd * lnmarkets_response[:body]['margin']).round(0) * 100.0).round(0)
          margin_percent_of_quantity = (lnmarkets_response[:body]['margin'].to_f/quantity_btc_sats.to_f).round(4)

          trade_log = TradeLog.create(
            score_log_id: args[:score_log_id],
            external_id: lnmarkets_response[:body]['id'],
            exchange_name: 'lnmarkets',
            derivative_type: 'options',
            trade_type: 'buy',
            trade_direction: direction,
            quantity_usd_cents: (lnmarkets_response[:body]['quantity'] * 100.0),
            quantity_btc_sats: quantity_btc_sats,
            open_fee: lnmarkets_response[:body]['opening_fee'],
            close_fee: lnmarkets_response[:body]['closing_fee'],
            margin_quantity_btc_sats: lnmarkets_response[:body]['margin'],
            margin_quantity_usd_cents: margin_quantity_usd_cents,
            open_price: lnmarkets_response[:body]['forward'],
            creation_timestamp: lnmarkets_response[:body]['creation_ts'],
            instrument: instrument_name,
            settlement: settlement,
            implied_volatility: lnmarkets_response[:body]['volatility'],
            running: true,
            closed: false,
            margin_percent_of_quantity: margin_percent_of_quantity
          )
        else
          Rails.logger.error(
            {
              message: "Error. Unable to open options contract.",
              body: "#{lnmarkets_response}",
              script: "lnmarkets_trader:open_options_contract"
            }.to_json
          )
        end
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch account balance info... skip trade.",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )
      end
    else
      Rails.logger.fatal(
        {
          message: "Error. Invocation missing required params.",
          script: "lnmarkets_trader:open_options_contract"
        }.to_json
      )
      abort 'Unable to invoke open_options_contract script.'
    end

    Rails.logger.info(
      {
        message: "End lnmarkets_trader:open_options_contract...",
        script: "lnmarkets_trader:open_options_contract"
      }.to_json
    )
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task check_stops: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    timestamp_current = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    timestamp_yesterday = timestamp_current - 86400000
    Rails.logger.info(
      {
        message: "Run lnmarkets_trader:check_stops...",
        script: "lnmarkets_trader:check_stops"
      }.to_json
    )
    # Initialize lnmarkets_client
    lnmarkets_client = LnmarketsAPI.new

    #
    # 1. Check for any running futures positions...
    #
    running_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('running', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      running_futures = lnmarkets_response[:body]
      if running_futures.count > 0
        Rails.logger.info(
          {
            message: "Running Futures: #{running_futures.count}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
      end
    else
      Rails.logger.fatal(
        {
          message: "Error. Unable to get running futures trades.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    if running_futures.any?
      Rails.logger.info(
        {
          message: "Evaluate if each running futures trade needs its stop-loss updated...",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
      #
      # Fetch latest price of BTCUSD
      #
      index_price_btcusd = 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        Rails.logger.info(
          {
            message: "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch latest price for BTCUSD... abort check_stops script.",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        abort 'Unable to proceed with updating Stops without BTCUSD price.'
      end

      #
      # Iterate through each futures trade
      #
      running_futures.each do |f|
        puts ''
        puts '---------------------------------------------------'
        puts '---------------------------------------------------'
        puts ''
        Rails.logger.info(
          {
            message: "Futures Trade ID: #{f['id']}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        #
        # Check trade direction, long/short
        #
        trade_direction = ''
        if f['side'] == 'b'
          trade_direction = 'long'
        elsif f['side'] == 's'
          trade_direction = 'short'
        end
        Rails.logger.info(
          {
            message: "Trade Direction: #{trade_direction}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        #
        # Check if position is 'in the money'
        #
        update_trade_stoploss_price = false
        entry_price = f['entry_price']
        previous_stoploss = f['stoploss']
        Rails.logger.info(
          {
            message: "Trade Entry Price: #{entry_price.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        Rails.logger.info(
          {
            message: "Trade Previous Stoploss: #{previous_stoploss.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        if trade_direction == 'long'
          if index_price_btcusd > entry_price
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Update stop-loss for #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            update_trade_stoploss_price = true
          else
            next
          end
        elsif trade_direction == 'short'
          if index_price_btcusd < entry_price
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Update stop-loss for #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            update_trade_stoploss_price = true
          else
            next
          end
        end

        #
        # 4. Update the position's stop-loss
        #
        if update_trade_stoploss_price == true
          Rails.logger.info(
            {
              message: "Attempt to update futures trade...",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
          #
          # Calcualte new stoploss
          #
          if trade_direction == 'long'
            if index_price_btcusd > (entry_price * 1.035)
              new_stoploss = (entry_price * 1.035).round(0)
            elsif index_price_btcusd > (entry_price * 1.026)
              new_stoploss = (entry_price * 1.026).round(0)
            elsif index_price_btcusd > (entry_price * 1.025)
              new_stoploss = (entry_price * 1.025).round(0)
            elsif index_price_btcusd > (entry_price * 1.02)
              new_stoploss = (entry_price * 1.01).round(0)
            elsif index_price_btcusd > (entry_price * 1.015)
              new_stoploss = (entry_price * 1.0).round(0)
            elsif index_price_btcusd > (entry_price * 1.01)
              new_stoploss = (entry_price * 0.99).round(0)
            else
              new_stoploss = (index_price_btcusd * 0.97).round(0)
            end
          elsif trade_direction == 'short'
            if index_price_btcusd < (entry_price * 0.965)
              new_stoploss = (entry_price * 0.965).round(0)
            elsif index_price_btcusd < (entry_price * 0.974)
              new_stoploss = (entry_price * 0.974).round(0)
            elsif index_price_btcusd < (entry_price * 0.975)
              new_stoploss = (entry_price * 0.975).round(0)
            elsif index_price_btcusd < (entry_price * 0.98)
              new_stoploss = (entry_price * 0.99).round(0)
            elsif index_price_btcusd < (entry_price * 0.985)
              new_stoploss = (entry_price * 1.0).round(0)
            elsif index_price_btcusd < (entry_price * 0.99)
              new_stoploss = (entry_price * 1.01).round(0)
            else
              new_stoploss = (index_price_btcusd * 1.03).round(0)
            end
          end
          Rails.logger.info(
            {
              message: "New stoploss: #{new_stoploss.to_fs(:delimited)}  (Previously: #{previous_stoploss})",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )

          if new_stoploss != previous_stoploss
            lnmarkets_response = lnmarkets_client.update_futures_trade(f['id'], 'stoploss', new_stoploss)
            if lnmarkets_response[:status] == 'success'
              Rails.logger.info(
                {
                  message: "Updated stoploss for #{f['id']}:",
                  body: "#{lnmarkets_response[:body].inspect}",
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
            else
              Rails.logger.error(
                {
                  message: "Error. Unable to update futures trade.",
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
            end
          else
            Rails.logger.warn(
              {
                message: "Warning. New stoploss is equal to previous stoploss. Skipped updating futures trade.",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
          end
        else
          Rails.logger.info(
            {
              message: "Trade is not in the money. Do no update stoploss.",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end
      end
      puts '---------------------------------------------------'
      puts '---------------------------------------------------'
    else
      Rails.logger.info(
        {
          message: "Skip. No running futures trades.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    #
    # 2. Check for any running options contracts...
    #
    running_contracts = []
    lnmarkets_response = lnmarkets_client.get_options_trades()
    if lnmarkets_response[:status] == 'success'
      all_contracts = lnmarkets_response[:body]
      if all_contracts.count > 0
        running_contracts = all_contracts.select { |a| a['running'] == true }
        Rails.logger.info(
          {
            message: "Running Options Contracts: #{running_contracts.count}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get running contracts.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    if running_contracts.any?
      Rails.logger.info(
        {
          message: "Evaluate if each running options contract should close early...",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
      #
      # Fetch latest price of BTCUSD
      #
      index_price_btcusd = 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        Rails.logger.info(
          {
            message: "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch latest price for BTCUSD... abort check_stops script.",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        abort 'Unable to proceed with updating Stops without BTCUSD price.'
      end

      #
      # Iterate through each options contract
      #
      running_contracts.each do |c|
        Rails.logger.info(
          {
            message: "Options Contract ID: #{c['id']}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        close_running_contract = false
        entry_price = c['forward']
        Rails.logger.info(
          {
            message: "Trade Entry Price: #{entry_price.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )

        trade_direction = ''
        if c['type'] == 'c'
          trade_direction = 'long'
        elsif c['type'] == 'p'
          trade_direction = 'short'
        end

        if trade_direction == 'long'
          if index_price_btcusd > (entry_price * 1.0355)
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Close In-the-Money options contract #{c['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            close_running_contract = true
          else
            next
          end
        elsif trade_direction == 'short'
          if index_price_btcusd < (entry_price * 0.9645)
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Close In-the-Money options contract #{c['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            close_running_contract = true
          else
            next
          end
        end

        if close_running_contract == true
          lnmarkets_response = lnmarkets_client.close_options_contract(c['id'])

          if lnmarkets_response[:status] == 'success'
            puts ""
            puts "Finished closing open options contract: #{c['id']}."
            puts ""
            #
            # Update Trade Log
            #
            trade_log = TradeLog.find_by_external_id(c['id'])
            if trade_log.present?
              trade_log.update(
                running: false,
                closed: true
              )
              trade_log.get_final_trade_stats
            else
              Rails.logger.error(
                {
                  message: "Error. Unable to find trade log for trade: #{c['id']}",
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
            end
          else
            Rails.logger.error(
              {
                message: "Error. Unable to close open options contracts: #{c['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
          end
        else
          Rails.logger.info(
            {
              message: "Allow options contract to continue running. #{c['id']}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No running options contracts.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    Rails.logger.info(
      {
        message: "End lnmarkets_trader:check_stops...",
        script: "lnmarkets_trader:check_stops"
      }.to_json
    )
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end
