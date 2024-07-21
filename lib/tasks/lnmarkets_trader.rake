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
    last_8_market_data_log_entries = MarketDataLog.last(8).pluck(:aggregate_open_interest)
    if last_8_market_data_log_entries != nil && !last_8_market_data_log_entries.empty?
      last_8_aggregate_open_interests_average = last_8_market_data_log_entries.sum.fdiv(last_8_market_data_log_entries.size).round(2)
    else
      last_8_aggregate_open_interests_average = 0.0
      data_errors += 1
    end

    # Last 10 1D Candle Close Avg
    last_10_candle_closes_average = 0.0
    start_date = (timestamp - (9 * 86400000))
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
    puts "LAST BTCUSD TICK:"
    puts btcusd
    puts ""

    #
    # Save MarketDataLog
    #
    begin
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        price_btcusd: btcusd,
        prior_day_volume: current_day_volume,
        two_days_ago_volume: prior_day_volume,
        three_days_ago_volume: two_days_ago_volume,
        four_days_ago_volume: three_days_ago_volume,
        five_days_ago_volume: four_days_ago_volume,
        six_days_ago_volume: five_days_ago_volume,
        seven_days_ago_volume: six_days_ago_volume,
        rsi: rsi_values[0]['value'],
        simple_moving_average: simple_moving_average_values[0]['value'],
        exponential_moving_average: exponential_moving_average_values[0]['value'],
        macd_value: macd_values[0]['value'],
        macd_signal: macd_values[0]['signal'],
        macd_histogram: macd_values[0]['histogram'],
        avg_funding_rate: avg_funding_rate,
        aggregate_open_interest: aggregate_open_interest,
        avg_last_10_candle_closes: last_10_candle_closes_average,
        avg_last_8_aggregate_open_interests: last_8_aggregate_open_interests_average
      )
    rescue => e
      puts e
      puts 'Error saving market_data_log record'
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

    #
    # Save ScoreLog
    #
    puts ""
    puts "Final Score: #{trade_direction_score}"
    puts ""
    begin
      score_log = ScoreLog.create(
        recorded_date: DateTime.now,
        market_data_log_id: market_data_log.id,
        score: trade_direction_score
      )
    rescue => e
      puts e
      puts 'Error saving score_log record'
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

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
      puts ""
      puts "Proceed with new trade direction: #{trade_direction}"
      

      # Initialize lnmarkets_client
      lnmarkets_client = LnmarketsAPI.new

      #
      # Close existing futures and options contracts
      #
      puts ""
      puts "1. Check existing open options contracts..."
      puts "--------------------------------------------"
      lnmarkets_response = lnmarkets_client.get_options_trades()
      if lnmarkets_response[:status] == 'success'
        all_contracts = lnmarkets_response[:body]
        if all_contracts.count > 0
          running_contracts = all_contracts.select { |a| a['running'] == true }
          puts "Running Options Contracts: #{running_contracts.count}"
        else
          # No options trades returned
        end
      else
        puts 'Error. Unable to get running contracts.'
      end

      if running_contracts.any?
        puts ""
        puts "Close all open options contracts from prior trading interval..."
        puts ""
        #
        # Close all 'running' contracts
        #
        running_contracts.each do |c|
          lnmarkets_client.close_option_contract(c['id'])

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
                close_price: lnmarkets_response[:body]['fixing_price'],
                closed_timestamp: lnmarkets_response[:body]['closed_ts'],
                close_fee: lnmarkets_response[:body]['closing_fee'],
                absolute_net_proceeds: lnmarkets_response[:body]['pl'],
                running: false,
                closed: true
              )
            else
              puts "Error. Unable to find trade log for trade: #{f['id']}"
            end
          else
            puts "Error. Unable to close open options contracts: #{c['id']}"
          end
        end
      else
        puts ""
        puts "No running contracts."
        puts ""
      end

      puts ""
      puts "2. Check existing open and running futures trades..."
      puts "--------------------------------------------"
      lnmarkets_response = lnmarkets_client.get_futures_trades('open')
      if lnmarkets_response[:status] == 'success'
        open_futures = lnmarkets_response[:body]
        puts "Open Futures: #{open_futures.count}"
      else
        puts 'Error. Unable to get open futures trades.'
      end

      if open_futures.any?
        puts ""
        puts "Cancel all open futures trades from prior trading interval..."
        puts ""
        #
        # Cancel all open futures trades
        #
        open_futures.each do |f|
          lnmarkets_response = lnmarkets_client.cancel_futures_trade(f['id'])
          if lnmarkets_response[:status] == 'success'
            puts ""
            puts "Finished closing futures trade: #{f['id']}."
            puts ""
            #
            # Update Trade Log
            #
            trade_log = TradeLog.find_by_external_id(f['id'])
            if trade_log.present?
              trade_log.update(
                open: false,
                canceled: true,
                closed_timestamp: lnmarkets_response[:body]['closed_ts'],
                last_update_timestamp: lnmarkets_response[:body]['last_update_ts']
              )
            else
              puts "Error. Unable to find trade log for trade: #{f['id']}"
            end
          else
            puts "Error. Unable to close futures trade: #{f['id']}"
          end
        end
      else
        puts ""
        puts "No running futures trades."
        puts ""
      end

      lnmarkets_response = lnmarkets_client.get_futures_trades('running')
      if lnmarkets_response[:status] == 'success'
        running_futures = lnmarkets_response[:body]
        puts "Running Futures: #{running_futures.count}"
      else
        puts 'Error. Unable to get running futures trades.'
      end

      if running_futures.any?
        puts ""
        puts "Close all running futures trades from prior trading interval..."
        puts ""
        #
        # Close all futures trades
        #
        running_futures.each do |f|
          lnmarkets_response = lnmarkets_client.close_futures_trade(f['id'])
          if lnmarkets_response[:status] == 'success'
            puts ""
            puts "Finished closing futures trade: #{f['id']}."
            puts ""
            #
            # Update Trade Log
            #
            trade_log = TradeLog.find_by_external_id(f['id'])
            if trade_log.present?
              trade_log.update(
                close_price: lnmarkets_response[:body]['fixing_price'],
                closed_timestamp: lnmarkets_response[:body]['closed_ts'],
                close_fee: lnmarkets_response[:body]['closing_fee'],
                absolute_net_proceeds: lnmarkets_response[:body]['pl'],
                open: false,
                running: false,
                closed: true,
                last_update_timestamp: lnmarkets_response[:body]['last_update_ts']
              )
            else
              puts "Error. Unable to find trade log for trade: #{f['id']}"
            end
          else
            puts "Error. Unable to close futures trade: #{f['id']}"
          end
        end
      else
        puts ""
        puts "No running futures trades."
        puts ""
      end

      puts ""
      puts "3. Proceed to create new #{trade_direction} trade..."
      puts "--------------------------------------------"
      puts ""
      if trade_direction == 'buy'
        #Rake::Task["lnmarkets_trade_operations:create_long_trade"].execute({score_log_id: score_log.id})
      elsif trade_direction == 'sell'
        #Rake::Task["lnmarkets_trade_operations:create_short_trade"].execute({score_log_id: score_log.id})
      end
      puts ""
      puts "Finished creating new #{trade_direction} trade."
      puts ""
      puts "********************************************"
      puts "********************************************"
    elsif trade_direction_score == 0.0
      #
      # No trade... wait for updated market indicators
      #
      puts "No trade."
      puts ""
    end

    puts ""
    puts "Data Errors: #{data_errors}"
    puts ""
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
    puts "args[:score_log_id]: #{args[:score_log_id]}"
    if !args[:score_log_id].present?
      puts ""
      puts "Error. Invocation missing required params."
      puts ""
      return
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
      polygon_client = PolygonAPI.new
      price_btcusd = 0.00
      response_btcusd = polygon_client.get_last_trade('BTC', 'USD')
      if response_btcusd[:status] == 'success'
        price_btcusd = response_btcusd[:body]['last']['price']
        puts "Price BTCUSD: #{price_btcusd.to_fs(:delimited)}"

        price_sat_usd = (price_btcusd/100000000.0).round(5)
        balance_usd = (sats_balance * price_sat_usd).round(2)
        puts ""
        puts "Balance USD: #{balance_usd.to_fs(:delimited)}"
      else
        puts 'Error. Unable to fetch latest price for BTCUSD... skip trade.'
        return
      end

      #
      # Define leverage factor
      #
      leverage_factor = 2.50
      puts "Leverage: #{leverage_factor}"

      #
      # Determine capital waged
      #
      capital_waged_usd = (balance_usd * leverage_factor).round(2)
      puts "Capital Waged with Leverage: #{capital_waged_usd.to_fs(:delimited)}"

      #
      # Execute Buy Limit order
      #
      #
      side = 'b'
      type = 'l'
      leverage = 3
      price = (price_btcusd * 0.999925).round(0)
      quantity = capital_waged_usd
      takeprofit = (price_btcusd * 1.07).round(0)
      stoploss = (price_btcusd * 0.95).round(0)

      lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        #
        # Create new record in TradeLogs table
        #
        trade_log = TradeLog.create(
          score_log_id: score_log_id,
          external_id: lnmarkets_response[:body]['id'],
          exchange_name: 'lnmarkets',
          derivative_type: 'futures',
          trade_type: 'buy',
          trade_direction: 'long',
          quantity: lnmarkets_response[:body]['quantity'],
          open_fee: lnmarkets_response[:body]['opening_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          margin_quantity: lnmarkets_response[:body]['margin'],
          leverage_quantity: lnmarkets_response[:body]['leverage'],
          open_price: lnmarkets_response[:body]['price'],
          creation_timestamp: lnmarkets_response[:body]['creation_ts'],
          last_update_timestamp: lnmarkets_response[:body]['last_update_ts']
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'short', amount: lnmarkets_response[:body]['quantity'], score_log_id: score_log_id})
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
    puts "args[:score_log_id]: #{args[:score_log_id]}"
    if !args[:score_log_id].present?
      puts ""
      puts "Error. Invocation missing required params."
      puts ""
      return
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
      polygon_client = PolygonAPI.new
      price_btcusd = 0.00
      response_btcusd = polygon_client.get_last_trade('BTC', 'USD')
      if response_btcusd[:status] == 'success'
        price_btcusd = response_btcusd[:body]['last']['price']
        puts "Price BTCUSD: #{price_btcusd.to_fs(:delimited)}"

        price_sat_usd = (price_btcusd/100000000.0).round(5)
        balance_usd = (sats_balance * price_sat_usd).round(2)
        puts ""
        puts "Balance USD: #{balance_usd.to_fs(:delimited)}"
      else
        puts 'Error. Unable to fetch latest price for BTCUSD... skip trade.'
        return
      end

      #
      # Define leverage factor
      #
      leverage_factor = 2.75
      puts "Leverage: #{leverage_factor}"

      #
      # Determine capital waged
      #
      capital_waged_usd = (balance_usd * leverage_factor).round(2)
      puts "Capital Waged with Leverage: #{capital_waged_usd.to_fs(:delimited)}"

      #
      # Execute Short Limit order
      #
      #
      side = 's'
      type = 'l'
      leverage = 3
      price = (price_btcusd * 1.000025).round(0)
      quantity = capital_waged_usd
      takeprofit = (price_btcusd * 0.98).round(0)
      stoploss = (price_btcusd * 1.09).round(0)

      lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        #
        # Create new record in TradeLogs table
        #
        trade_log = TradeLog.create(
          score_log_id: args[:score_log_id],
          external_id: lnmarkets_response[:body]['id'],
          exchange_name: 'lnmarkets',
          derivative_type: 'futures',
          trade_type: 'sell',
          trade_direction: 'short',
          quantity: lnmarkets_response[:body]['quantity'],
          open_fee: lnmarkets_response[:body]['open_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          margin_quantity: lnmarkets_response[:body]['margin'],
          leverage_quantity: lnmarkets_response[:body]['leverage'],
          open_price: lnmarkets_response[:body]['price'],
          open_fee: lnmarkets_response[:body]['opening_fee'],
          creation_timestamp: lnmarkets_response[:body]['creation_ts'],
          last_update_timestamp: lnmarkets_response[:body]['last_update_ts']
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'long', amount: lnmarkets_response[:body]['quantity'], score_log_id: score_log_id})
      else
        puts 'Error. Unable to create futures trade.'
      end
    else
      puts 'Error. Unable to fetch account balance info... skip trade.'
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
    puts ''
    puts 'Run lnmarkets_trader:open_options_contract...'
    puts ''
    puts "args[:direction]: #{args[:direction]}"
    puts "args[:amount]: #{args[:amount]}"
    puts "args[:score_log_id]: #{args[:score_log_id]}"
    if args[:direction].present? && 
      args[:amount].present? &&
      args[:score_log_id].present?

      if ['long', 'short'].include?(args[:direction])
        direction = args[:direction]
      else
        puts 'Error. Invalid trade direction parameter.'
        return
      end

      # Assign capital waged based on Lnmarkets trading limits
      if args[:amount] > 500000.00
        capital_waged_usd = args[:amount]
      else
        puts ""
        puts "Quantity too high for options trade. Adjusting to 499,999.00"
        puts ""
        capital_waged_usd = 499999.00
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
        puts "Sats Available: #{sats_balance.to_fs(:delimited)}"
        puts ""

        #
        # Fetch latest price of BTCUSD
        #
        polygon_client = PolygonAPI.new
        price_btcusd = 0.00
        response_btcusd = polygon_client.get_last_trade('BTC', 'USD')
        if response_btcusd[:status] == 'success'
          price_btcusd = response_btcusd[:body]['last']['price']

          price_sat_usd = (price_btcusd/100000000.0).round(5)
          balance_usd = (sats_balance * price_sat_usd).round(2)
        else
          puts 'Error. Unable to fetch latest price for BTCUSD.'
          return
        end

        #
        # Verify available balance to satisfy margin requirement
        # - About 2% of capital waged is required for In-the-Money options
        #
        approx_margin_requirement_usd = (capital_waged_usd * 0.02).round(2)
        if approx_margin_requirement_usd < balance_usd
          puts 'Error. Not enough balance available to attempt trade.'
          return
        end

        #
        # 2. Fetch options instruments
        #
        lnmarkets_response = lnmarkets_client.get_options_instruments
        if lnmarkets_response[:status] == 'success'
          filtered_instruments = lnmarkets_response[:body].select {|y| y.include?((DateTime.now + 1.day).utc.strftime("BTC.%Y-%m-%d")) }

          if direction == 'long'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.C') }
            filtered_instruments = filtered_instruments.select { |y| y.include?((price_btcusd-1000).ceil(-3).to_s) }
          elsif direction == 'short'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.P') }
            filtered_instruments = filtered_instruments.select { |y| y.include?((price_btcusd).ceil(-3).to_s) }
          end
        else
          puts 'Error. Unable to fetch options instruments.'
          return
        end

        if filtered_instruments.any?
          puts ''
          puts 'Found options instrument:'
          puts "#{filtered_instruments[0]}"
        else
          puts 'No viable options instruments found.'
          return
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
          puts lnmarkets_response
          #
          # Create new record in TradeLogs table
          #
          trade_log = TradeLog.create(
            score_log_id: args[:score_log_id],
            external_id: lnmarkets_response[:body]['id'],
            exchange_name: 'lnmarkets',
            derivative_type: 'options',
            trade_type: 'buy',
            trade_direction: direction,
            quantity: lnmarkets_response[:body]['quantity'],
            open_fee: lnmarkets_response[:body]['opening_fee'],
            close_fee: lnmarkets_response[:body]['closing_fee'],
            margin_quantity: lnmarkets_response[:body]['margin'],
            open_price: lnmarkets_response[:body]['forward'],
            creation_timestamp: lnmarkets_response[:body]['creation_ts'],
            running: true,
            closed: false
          )
        else
          puts 'Error. Unable to open options contract.'
          puts lnmarkets_response
        end
      else
        puts 'Error. Unable to fetch account balance info... skip trade.'
      end
    else
      puts ""
      puts "Error. Invocation missing required params."
      puts ""
      return
    end

    puts 'End lnmarkets_trader:open_options_contract...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task check_stops: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:check_stops...'
    puts ''
    # Initialize lnmarkets_client
    lnmarkets_client = LnmarketsAPI.new
    puts ''
    puts 'End lnmarkets_trader:check_stops...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end
