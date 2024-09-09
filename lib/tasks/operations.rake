namespace :operations do
  task generate_thirty_minute_training_data: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts "Begin operations:generate_thirty_minute_training_data..."
    #
    # Every 30mins collect new market data and 1x per day update model
    #
    #
    # Initialize BigQuery client
    #
    require "google/cloud/bigquery"
    PROJECT_ID = "encrypted-energy"
    bigquery = Google::Cloud::Bigquery.new(project: PROJECT_ID)

    #
    # Set dataset, table, and table names
    #
    DATASET_ID = "market_indicators"
    TABLE_ID = "thirty_minute_training_data"
    MODEL_ID = "random_forest"

    # Timestamp
    # time_now_utc = Time.now.utc
    # most_recent_30min_interval = time_now_utc.change(
    #   min: time_now_utc.min < 30 ? 0 : 30
    # )
    # timestamp_milliseconds = most_recent_30min_interval.to_i.in_milliseconds

    #
    # Fetch last date in the training data table
    #
    query = "SELECT timestamp FROM `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}` ORDER BY timestamp DESC LIMIT 1"
    results = bigquery.query(query)
    last_timestamp = results.first ? results.first[:timestamp] : nil

    if last_timestamp
      puts "Last entry in the training data table: #{last_timestamp}"
    else
      puts "No entries found in the training data table"
    end

    #
    # Assign start date based on the last timestamp entry
    #
    parsed_last_timestamp = Time.parse(last_timestamp.to_s)

    start_timestamp_milliseconds = (parsed_last_timestamp.to_i.in_milliseconds + 30.minutes.to_i.in_milliseconds)
    end_timestamp_milliseconds = (DateTime.now.utc.beginning_of_hour).to_i.in_milliseconds

    #
    # Loop through each 30min interval and fetch market indicators
    #
    while start_timestamp_milliseconds <= end_timestamp_milliseconds
      #
      # Fetch market indicators from Polygon and other sources
      #
      # Collect the following market indicators:
      # timestamp milliseconds, rsi, volume, sma, ema, macd, candle open, 
      # candle close, candle low, candle high, price index, price coinbase, 
      # price binance, and whether or not the price increased
      #{"timestamp":, "rsi":, "volume":, "sma":, "ema":, "macd_histogram":, "candle_open":, "candle_close":, "candle_low":, "candle_high":, "price_btcusd_index":, "price_btcusd_coinbase":, "price_btcusd_binance":, "price_direction":,"implied_volatility_t3":, "avg_funding_rate":, "aggregate_open_interest":, "avg_long_short_ratio":}
      
      # Initialize shared inputs
      polygon_client = PolygonAPI.new
      lnmarkets_client = LnMarketsAPI.new
      coinalyze_client = CoinalyzeAPI.new
      symbol = 'X:BTCUSD'
      timespan = 'minute'
      window = 30
      series_type = 'close'

      # RSI
      rsi = 0.0
      response_rsi = polygon_client.get_rsi(symbol, start_timestamp_milliseconds, timespan, window, series_type)
      if response_rsi[:status] == 'success'
        rsi = response_rsi[:body]['results']['values'][0]['value'].round(2)
      end

      # Volume, Candle Open, Candle Close, Candle Low, Candle High
      volume = 0.0
      candle_open = 0.0
      candle_close = 0.0
      candle_high = 0.0
      candle_low = 0.0
      aggregates_timespan = 'minute'
      aggregates_multiplier = 30
      start_date = (start_timestamp_milliseconds - 30.minutes.to_i.in_milliseconds)
      end_date = start_timestamp_milliseconds
      response_volume = polygon_client.get_aggregate_bars(symbol, aggregates_timespan, aggregates_multiplier, start_date, end_date)
      if response_volume[:status] == 'success'
        volume = response_volume[:body]['results'][1]['v'].round(2)
        candle_open = response_volume[:body]['results'][1]['o'].round(2)
        candle_close = response_volume[:body]['results'][1]['c'].round(2)
        candle_high = response_volume[:body]['results'][1]['h'].round(2)
        candle_low = response_volume[:body]['results'][1]['l'].round(2)
      end

      # SMA
      simple_moving_average = 0.0
      response_sma = polygon_client.get_sma(symbol, start_timestamp_milliseconds, timespan, window, series_type)
      if response_sma[:status] == 'success'
        simple_moving_average = response_sma[:body]['results']['values'][0]['value'].round(2)
      end

      # EMA
      exponential_moving_average = 0.0
      response_ema = polygon_client.get_ema(symbol, start_timestamp_milliseconds, timespan, window, series_type)
      if response_ema[:status] == 'success'
        exponential_moving_average = response_ema[:body]['results']['values'][0]['value'].round(2)
      end

      # MACD
      macd_histogram = 0.0
      short_window = 120
      long_window = 260
      signal_window = 30
      response_macd = polygon_client.get_macd(symbol, start_timestamp_milliseconds, timespan, short_window, long_window, signal_window, series_type)
      if response_macd[:status] == 'success'
        macd_histogram = response_macd[:body]['results']['values'][0]['histogram'].round(2)
      end

      # Price BTCUSD Index
      price_btcusd_index = candle_open
      # start_timestamp_milliseconds_minus_one_minute = ((start_timestamp_milliseconds - 1.minutes.to_i.in_milliseconds)).round(0)
      # lnmarkets_response = lnmarkets_client.get_price_btcusd_index_history(start_timestamp_milliseconds_minus_one_minute, start_timestamp_milliseconds)
      # if lnmarkets_response[:status] == 'success'
      #   puts lnmarkets_response[:body]
      #   price_btcusd_index = lnmarkets_response[:body][0]['index'].round(2)
      # end
      
      # Price BTCUSD Coinbase
      price_btcusd_coinbase, price_btcusd_binance = 0.0, 0.0
      # start_timestamp_nanoseconds = start_timestamp_milliseconds * 1_000_000
      # response_btc_usd_trades = polygon_client.get_trades(symbol, start_timestamp_nanoseconds)
      # if response_macd[:status] == 'success'
      #   # Exchange id 1 is Coinbase
      #   # Exchange id 10 is Binance
      #   response_btc_usd_trades[:body]['results'].each do |trade|
      #     if trade['conditions'].include?(2) && trade['exchange'] == 1
      #       price_btcusd_coinbase = trade['price']
      #       break
      #     end
      #   end
      #   response_btc_usd_trades[:body]['results'].each do |trade|
      #     if trade['conditions'].include?(2) && trade['exchange'] == 10
      #       price_btcusd_binance = trade['price']
      #       break
      #     end
      #   end
      #   if coinbase_price_btcusd == 0.0
      #     price_btcusd_coinbase = price_btcusd_index
      #   end
      #   if binance_price_btcusd == 0.0
      #     price_btcusd_binance = price_btcusd_index
      #   end
      # end

      # Implied Volatility T3
      implied_volatility_t3 = 0.0
      t3_client = T3IndexAPI.new
      current_tick = Time.at(start_timestamp_milliseconds / 1000).utc.strftime("%Y-%m-%d-%H-%M-%S")
      t3_response = t3_client.get_tick(current_tick)
      if t3_response[:status] == 'success'
        puts "T3 RESPONSE:"
        puts t3_response[:body]
        implied_volatility_t3 = t3_response[:body]['value']
      end

      # Avg Funding Rate
      # Fix - Not getting any results
      avg_funding_rate = 0.0
      # start_timestamp_seconds = ((start_timestamp_milliseconds - 30.minutes.to_i.in_milliseconds) / 1000.0).round(0)
      # end_timestamp_seconds = ((start_timestamp_milliseconds) / 1000.0).round(0)
      # interval = "30min"
      # symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6'
      # begin
      #   coinalyze_response = coinalyze_client.get_avg_funding_history(symbols, interval, start_timestamp_seconds, end_timestamp_seconds)
      #   if coinalyze_response[:body].count > 0
      #     coinalyze_response[:body].each_with_index do |f, index|
      #       avg_funding_rate += coinalyze_response[:body][index]['history'][0]['c']
      #     end
      #     avg_funding_rate = (avg_funding_rate/coinalyze_response[:body].count).round(4)
      #   end
      # rescue => e
      #   puts e
      #   puts 'Error fetching funding rate data'
      # end
      # sleep(5)

      # Aggregate Open Interest
      aggregate_open_interest = 0.0
      # start_timestamp_seconds = ((start_timestamp_milliseconds - 30.minutes.to_i.in_milliseconds) / 1000.0).round(0)
      # end_timestamp_seconds = ((start_timestamp_milliseconds) / 1000.0).round(0)
      # interval = "30min"
      # symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6,BTC-PERP.V,BTC_USDT.Y,BTC_USDC-PERPETUAL.2,BTCUSDC_PERP.A,BTCUSDT_PERP.F,BTC-USD.8,BTC_USD.Y,BTC-PERPETUAL.2,BTCUSDT_PERP.3,BTCEURT_PERP.F,BTCUSDU24.6,BTCUSDZ24.6'
      # begin
      #   coinalyze_response = coinalyze_client.get_avg_open_interest_history(symbols, interval, start_timestamp_seconds, end_timestamp_seconds)
      #   if coinalyze_response[:body].count > 0
      #     coinalyze_response[:body].each_with_index do |f, index|
      #       aggregate_open_interest += coinalyze_response[:body][index]['history'][0]['c']
      #     end
      #     aggregate_open_interest = aggregate_open_interest.round(1)
      #   end
      # rescue => e
      #   puts e
      #   puts 'Error fetching open interest data'
      # end
      # sleep(5)

      # Avg Long Short Ratio
      avg_long_short_ratio = 0.0
      # start_timestamp_seconds = ((start_timestamp_milliseconds - 30.minutes.to_i.in_milliseconds) / 1000.0).round(0)
      # end_timestamp_seconds = ((start_timestamp_milliseconds) / 1000.0).round(0)
      # interval = "30min"
      # symbols = 'BTCUSD.6,BTCUSD.7,BTCUSDT.6,BTCUSD_PERP.A,BTCUSDT_PERP.A,BTCUSD_PERP.0,BTCUSDC_PERP.3,BTCUSD_PERP.4,BTCUSDT_PERP.4,BTCUSDH24.6,BTC-PERP.V,BTC_USDT.Y,BTC_USDC-PERPETUAL.2,BTCUSDC_PERP.A,BTCUSDT_PERP.F,BTC-USD.8,BTC_USD.Y,BTC-PERPETUAL.2,BTCUSDT_PERP.3,BTCEURT_PERP.F,BTCUSDU24.6,BTCUSDZ24.6'
      # begin
      #   coinalyze_response = coinalyze_client.get_long_short_ratio_history(symbols, interval, start_timestamp_seconds, end_timestamp_seconds)
      #   if coinalyze_response[:body].count > 0
      #     count_of_records = 0.0
      #     coinalyze_response[:body].each_with_index do |f, index|
      #       coinalyze_response[:body][index]['history'].each do |o|
      #         count_of_records += 1.0
      #         avg_long_short_ratio += o['r']
      #       end
      #     end
      #     avg_long_short_ratio = (avg_long_short_ratio / count_of_records).round(3)
      #   end
      # rescue => e
      #   puts e
      #   puts 'Error fetching long/short ratio data'
      # end
      # sleep(5)

      # Price Direction
      if candle_close > candle_open
        price_direction = 'up'
      else
        price_direction = 'down'
      end

      
      #
      # Format timestamp
      #
      formatted_timestamp_milliseconds = Time.at(start_timestamp_milliseconds / 1000.0).utc.strftime('%Y-%m-%d %H:%M:%S.%6N')

      #
      # Prepare new data to insert to table
      #
      new_data = {
        timestamp: formatted_timestamp_milliseconds,
        rsi: rsi,
        volume: volume,
        simple_moving_average: simple_moving_average,
        exponential_moving_average: exponential_moving_average,
        macd_histogram: macd_histogram,
        candle_open: candle_open,
        candle_close: candle_close,
        candle_low: candle_low,
        candle_high: candle_high,
        price_btcusd_index: price_btcusd_index,
        price_btcusd_coinbase: price_btcusd_coinbase,
        price_btcusd_binance: price_btcusd_binance,
        avg_funding_rate: avg_funding_rate,
        aggregate_open_interest: aggregate_open_interest,
        implied_volatility_t3: implied_volatility_t3,
        avg_long_short_ratio: avg_long_short_ratio,
        price_direction: price_direction
      }
      row = new_data

      #
      # Insert new data to table
      #
      dataset = bigquery.dataset(DATASET_ID)
      table = dataset.table(TABLE_ID)
      table.insert row

      puts "Inserted new data: #{new_data}"
      start_timestamp_milliseconds += 30.minutes.to_i.in_milliseconds
      sleep(1.5)
    end

    #
    # Make prediction
    #
    query = <<-SQL
      WITH latest_data AS (
        SELECT
          timestamp,
          rsi,
          volume,
          simple_moving_average,
          exponential_moving_average,
          macd_histogram,
          candle_open,
          candle_close,
          candle_low,
          candle_high,
          price_btcusd_index,
          price_btcusd_coinbase,
          price_btcusd_binance,
          avg_funding_rate,
          aggregate_open_interest,
          implied_volatility_t3,
          avg_long_short_ratio,
          price_increased
        FROM
          `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
        ORDER BY timestamp DESC
        LIMIT 1
      )
      SELECT
        *
      FROM
        ML.PREDICT(MODEL `#{PROJECT_ID}.#{DATASET_ID}.#{MODEL_ID}`,
          (SELECT * FROM latest_data)
        );
    SQL

    #
    # Log prediction result
    #
    results = bigquery.query query
    results.each do |row|
      puts row.to_json
    end

    Rake::Task["lnmarkets_trader:attempt_trade_thirty_minute_trend"].execute({prediction: 'test'})

    #
    # 1x per day... retrain model if script is being run within 3 minutes of 04:00 UTC
    #
    if (Time.now.utc - Time.utc(Time.now.utc.year, Time.now.utc.month, Time.now.utc.day, 4, 0, 0)).abs <= 180
      puts "Starting daily model retraining..."

      query = <<-SQL
        CREATE OR REPLACE MODEL `#{PROJECT_ID}.#{DATASET_ID}.#{MODEL_ID}`
        OPTIONS(model_type='RANDOM_FOREST_CLASSIFIER',
                input_label_cols=['price_direction']) AS
        SELECT
          timestamp,
          rsi,
          volume,
          simple_moving_average,
          exponential_moving_average,
          macd_histogram,
          candle_open,
          candle_close,
          candle_low,
          candle_high,
          price_btcusd_index,
          price_btcusd_coinbase,
          price_btcusd_binance,
          avg_funding_rate,
          aggregate_open_interest,
          implied_volatility_t3,
          avg_long_short_ratio,
          price_direction
        FROM
          `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
        WHERE
          timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);
      SQL

      job = bigquery.query_job query
      job.wait_until_done!

      if job.error?
        puts "Error retraining model: #{job.error}"
      else
        puts "Model retrained successfully"
      end
      puts "End daily model retraining."
    end
    #
    # End
    #
    puts "End operations:generate_thirty_minute_training_data"
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task update_missing_daily_market_data: :environment do
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
    lnmarkets_client = LnMarketsAPI.new
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