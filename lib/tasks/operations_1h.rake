namespace :operations do
    task generate_hourly_training_data_previous_interval: :environment do
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts "Begin operations:generate_hourly_training_data_previous_interval..."
      #
      # Every 1d collect new market data and 1x per week update model
      #
      #
      # Initialize BigQuery client
      #
      require "google/cloud/bigquery"
      PROJECT_ID = "encrypted-energy"
  
      bigquery = if Rails.env.production?
                    credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
                    bigquery = Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
                  else
                     bigquery = Google::Cloud::Bigquery.new(project: PROJECT_ID)
                  end
  
      #
      # Set dataset, table, and table names
      #
      DATASET_ID = "market_indicators"
      TABLE_ID = "hourly_training_data"
  
      #
      # Fetch last date in the training data table
      #
      query = "SELECT timestamp_close FROM `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}` WHERE candle_open IS NOT NULL AND candle_close IS NULL ORDER BY timestamp_close ASC LIMIT 1"
      results = bigquery.query(query)
      last_timestamp_close = results.first ? results.first[:timestamp_close] : nil
  
      if last_timestamp_close
        puts "Last entry in the training data table: #{last_timestamp_close}"
      else
        abort "No entries found in the training data table"
      end
  
      #
      # Assign initial loop start date based on the last timestamp entry with candle_open value but no candle_close value
      #
      parsed_last_timestamp = Time.parse(last_timestamp_close.to_s).utc
      loop_start_timestamp_milliseconds = (parsed_last_timestamp.to_i.in_milliseconds - 1.hour.to_i.in_milliseconds)
  
      time_now_utc = Time.now.utc
      most_recent_1h_interval = time_now_utc.change(
        min: time_now_utc.min < 60 ? 0 : 60
      )
      loop_end_timestamp_milliseconds = most_recent_1h_interval.to_i.in_milliseconds
      last_loop_start_timestamp_milliseconds = (loop_end_timestamp_milliseconds - 1.hour.to_i.in_milliseconds)
      minutes_since_loop_end_interval = ((time_now_utc - most_recent_1h_interval) / 60).to_i
  
      #
      # Loop through each 1d interval and fetch interval Close market indicators
      #   + Make prediction if latest start timestamp is within the last 30 minutes
      #
      while loop_start_timestamp_milliseconds <= last_loop_start_timestamp_milliseconds
        puts "Loop start timestamp milliseconds: #{loop_start_timestamp_milliseconds}"
        puts ""
        #
        # Fetch relevant row
        #
        interval_timestamp_close = loop_start_timestamp_milliseconds + 1.hour.to_i.in_milliseconds
        time_obj_interval_timestamp_close = Time.at(interval_timestamp_close / 1000.0).utc
        query = "SELECT id FROM `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}` WHERE timestamp_close = '#{time_obj_interval_timestamp_close}' LIMIT 1"
        results = bigquery.query(query)
        row_id = results.first ? results.first[:id] : nil
  
        if row_id
          puts "Update row #{row_id} in #{TABLE_ID}."
        else
          abort "No entries found in the training data table"
        end
  
        #
        # Fetch market indicators from Polygon and other sources
        #
        # Initialize shared inputs
        polygon_client = PolygonAPI.new
        lnmarkets_client = LnMarketsAPI.new
        coinglass_client = CoinglassAPI.new
        t3_client = T3IndexAPI.new
  
        #
        # Fetch Close metrics
        #
  
        # Polygon inputs
        symbol_polygon = 'X:BTCUSD'
        timespan = 'minute'
        window = 60
        series_type = 'close'
  
        # RSI
        rsi_close = 0.0
        response_rsi = polygon_client.get_rsi(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_rsi[:status] == 'success' &&
          response_rsi[:body]['results']['values'].present?
          rsi_close = response_rsi[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          rsi_close = 0.0
        end
  
        # SMA
        simple_moving_average_close = 0.0
        response_sma = polygon_client.get_sma(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_sma[:status] == 'success' &&
          response_sma[:body]['results']['values'].present?
          simple_moving_average_close = response_sma[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          simple_moving_average_close = 0.0
        end
  
        # EMA
        exponential_moving_average_close = 0.0
        response_ema = polygon_client.get_ema(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_ema[:status] == 'success' &&
          response_ema[:body]['results']['values'].present?
          exponential_moving_average_close = response_ema[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          exponential_moving_average_close = 0.0
        end
  
        # MACD
        macd_histogram_close = 0.0
        short_window = 120
        long_window = 260
        signal_window = 30
        response_macd = polygon_client.get_macd(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, short_window, long_window, signal_window, series_type)
        if response_macd[:status] == 'success' &&
          response_macd[:body]['results']['values'].present?
          macd_histogram_close = response_macd[:body]['results']['values'][0]['histogram'].to_f.round(2)
        else
          macd_histogram_close = 0.0
        end
  
        # Volume, Candle Close, Candle High, Candle Low
        volume_open_to_close = 0.0
        candle_open, candle_close, candle_high, candle_low = 0.0, 0.0, 0.0, 0.0
        aggregates_timespan = 'hour'
        aggregates_multiplier = 1
        start_date = loop_start_timestamp_milliseconds
        end_date = (loop_start_timestamp_milliseconds + 1.hour.to_i.in_milliseconds)
        response_open_to_close_volume = polygon_client.get_aggregate_bars(
          symbol_polygon, aggregates_timespan, aggregates_multiplier, start_date, end_date)
        if response_open_to_close_volume[:status] == 'success' &&
          response_open_to_close_volume[:body]['resultsCount'] > 0
          volume_open_to_close = response_open_to_close_volume[:body]['results'][1]['v'].round(2).to_f
          candle_open = response_open_to_close_volume[:body]['results'][1]['o'].round(2).to_f
          candle_close = response_open_to_close_volume[:body]['results'][1]['c'].round(2).to_f
          candle_high = response_open_to_close_volume[:body]['results'][1]['h'].round(2).to_f
          candle_low = response_open_to_close_volume[:body]['results'][1]['l'].round(2).to_f
        else
          # No results
        end
  
        # Price Direction
        if candle_close > candle_open
          price_direction = 'up'
        else
          price_direction = 'down'
        end
  
        # Price BTCUSD Coinbase and Price BTCUSD Index
        price_btcusd_coinbase_close, price_btcusd_index_close = 0.0, 0.0
        #
        # Only fetch this data when script is being run within 10 minutes of interval start
        #
        if loop_start_timestamp_milliseconds == last_loop_start_timestamp_milliseconds &&
          minutes_since_loop_end_interval <= 10
          # Get Index
          lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
          if lnmarkets_response[:status] == 'success'
            price_btcusd_index = lnmarkets_response[:body]['index']
          else
            price_btcusd_index = 0.0
          end
          price_btcusd_index_close = price_btcusd_index.to_f.round(2)
  
          # Get Coinbase
          response_btc_usd_trades = polygon_client.get_trades(symbol_polygon)
          if response_macd[:status] == 'success'
            # Exchange id 1 is Coinbase
            # Exchange id 10 is Binance
            exchange_1_entry = response_btc_usd_trades[:body]["results"]
              .sort_by { |entry| -entry["participant_timestamp"].to_i }
              .find { |entry| entry["exchange"] == 1 && entry["conditions"].include?(2) }
  
            exchange_1_price = exchange_1_entry&.[]("price")
  
            if exchange_1_price == nil
              price_btcusd_coinbase_close = price_btcusd_index.to_f.round(2)
            else
              price_btcusd_coinbase_close = exchange_1_price.to_f.round(2)
            end
          end
        else
          puts 'Skip prices for Coinbase and Index since interval start timestamp not in the last 10 minutes.'
        end
  
        # Implied Volatility T3
        implied_volatility_t3_close = 0.0
        current_tick = Time.at(loop_start_timestamp_milliseconds / 1000).utc.strftime("%Y-%m-%d-%H-%M-%S")
        t3_response = t3_client.get_tick(current_tick)
        if t3_response[:status] == 'success'
          implied_volatility_t3_close = t3_response[:body]['value'].to_f.round(2)
        end
  
        #
        # CoinGlass Indicators - https://docs.coinglass.com/reference/version-10
        #
        symbol_coinglass = 'BTC'
        symbol_coinglass_long_short_ratio = 'BTCUSDT'
        start_timestamp_seconds = ((loop_start_timestamp_milliseconds) / 1000.0).round(0)
        end_timestamp_seconds = ((loop_start_timestamp_milliseconds + 30.minutes.to_i.in_milliseconds) / 1000.0).round(0)
        interval = "1h"
        exchange = "Binance"
  
        # Avg Funding Rate
        avg_funding_rate_close = 0.0
        coinglass_response = coinglass_client.get_aggregated_funding_rates(
          symbol_coinglass, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success'
          if avg_funding_rate_close = coinglass_response[:body]['data'][0].present?
            avg_funding_rate_close = coinglass_response[:body]['data'][0]['c'].to_f.round(4)
          else
            avg_funding_rate_close = 0.0
          end
        end
  
        # Aggregate Open Interest
        aggregate_open_interest_close = 0.0
        coinglass_response = coinglass_client.get_aggregated_open_interest(
          symbol_coinglass, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success' &&
          coinglass_response[:body]['data'].present?
          aggregate_open_interest_close = coinglass_response[:body]['data'][0]['c'].to_f.round(2)
        else
          aggregate_open_interest_close = 0.0
        end
  
        # Avg Long Short Ratio
        avg_long_short_ratio_close = 0.0
        coinglass_response = coinglass_client.get_accounts_long_short_ratio(
          exchange, symbol_coinglass_long_short_ratio, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success' &&
          coinglass_response[:body]['data'].present?
          avg_long_short_ratio_close = coinglass_response[:body]['data'][0]['longShortRatio'].to_f.round(4)
        else
          avg_long_short_ratio_close = 0.0
        end
  
        #
        # Update row
        #
        dataset = bigquery.dataset(DATASET_ID)
        table = dataset.table(TABLE_ID)
  
        #
        # Prep new data
        #
        updated_row = {
          rsi_close: rsi_close,
          volume_open_to_close: volume_open_to_close,
          simple_moving_average_close: simple_moving_average_close,
          exponential_moving_average_close: exponential_moving_average_close,
          macd_histogram_close: macd_histogram_close,
          candle_close: candle_close,
          candle_high: candle_high,
          candle_low: candle_low,
          price_btcusd_coinbase_close: price_btcusd_coinbase_close,
          price_btcusd_index_close: price_btcusd_index_close,
          avg_funding_rate_close: avg_funding_rate_close,
          aggregate_open_interest_close: aggregate_open_interest_close,
          implied_volatility_t3_close: implied_volatility_t3_close,
          avg_long_short_ratio_close: avg_long_short_ratio_close,
          price_direction: price_direction
        }
  
        # Construct the MERGE SQL statement
        update_set = updated_row.map { |k, v| "#{k} = @#{k}" }.join(", ")
        merge_sql = <<-SQL
          MERGE `#{table.project_id}.#{table.dataset_id}.#{table.table_id}` T
          USING (SELECT @id AS id) S
          ON T.id = S.id
          WHEN MATCHED THEN
            UPDATE SET #{update_set}
        SQL
  
        # Set up query parameters
        query_params = updated_row.merge(id: row_id)
  
        # Execute the MERGE operation with retries
        max_retries = 3
        retries = 0
  
        begin
          job = dataset.query_job(
            merge_sql,
            params: query_params
          )
          job.wait_until_done!
  
          if job.failed?
            puts "Error updating row: #{job.error}"
            raise job.error
          else
            puts "Row updated successfully"
          end
        rescue Google::Cloud::Error, HTTPClient::ReceiveTimeoutError => e
          if retries < max_retries
            retries += 1
            sleep(2 ** retries) # Exponential backoff
            retry
          else
            raise e
          end
        end
  
        loop_start_timestamp_milliseconds += 1.hour.to_i.in_milliseconds
        sleep(0.1)
      end
  
      # 1x per week on Sunday... retrain model if script is being run within 6 minutes of 00:00 UTC on Sunday
      if Time.now.utc.sunday? &&
        (Time.now.utc - Time.utc(Time.now.utc.year, Time.now.utc.month, Time.now.utc.day, 0, 0, 0)).abs <= 360
        puts "Starting hourly model retraining..."
        Rake::Task["operations:update_hourly_model"].execute()
        puts "Finished retraining hourly model."
      end
      puts "End operations:generate_hourly_training_data_previous_interval"
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    end
  
    task generate_hourly_training_data_next_interval: :environment do
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts "Begin operations:generate_hourly_training_data_next_interval..."
      #
      # Every 1d collect new market data and 1x per week update model
      #
      #
      # Initialize BigQuery client
      #
      require "google/cloud/bigquery"
      require "tempfile"
      PROJECT_ID = "encrypted-energy"
  
      bigquery = if Rails.env.production?
                    credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
                    bigquery = Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
                  else
                     bigquery = Google::Cloud::Bigquery.new(project: PROJECT_ID)
                  end
  
      #
      # Set dataset, table, and table names
      #
      DATASET_ID = "market_indicators"
      TABLE_ID = "hourly_training_data"
  
      #
      # Fetch last date in the training data table
      #
      query = "SELECT timestamp_open FROM `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}` ORDER BY timestamp_open DESC LIMIT 1"
      results = bigquery.query(query)
      last_timestamp_open = results.first ? results.first[:timestamp_open] : nil
  
      if last_timestamp_open
        puts "Last entry in the training data table: #{last_timestamp_open}"
      else
        abort "No entries found in the training data table"
      end
  
      #
      # Assign start date based on the last timestamp entry
      #
      parsed_last_timestamp = Time.parse(last_timestamp_open.to_s).utc
      loop_start_timestamp_milliseconds = (parsed_last_timestamp.to_i.in_milliseconds + 1.hour.to_i.in_milliseconds)
  
      time_now_utc = Time.now.utc
      time_now_beginning_of_hour_utc = Time.now.utc.beginning_of_hour
      most_recent_1h_interval = time_now_beginning_of_hour_utc.change(
        min: time_now_utc.min < 60 ? 0 : 60
      )
      loop_end_timestamp_milliseconds = most_recent_1h_interval.to_i.in_milliseconds
      last_loop_start_timestamp_milliseconds = loop_end_timestamp_milliseconds
      minutes_since_loop_end_interval = ((time_now_utc - most_recent_1h_interval) / 60).to_i
  
      #
      # Loop through each 1d interval and fetch market indicators
      #   + Make prediction if latest start timestamp is within the last 1 day
      #
      while loop_start_timestamp_milliseconds <= last_loop_start_timestamp_milliseconds
        puts "Loop start timestamp milliseconds: #{loop_start_timestamp_milliseconds}"
        puts ""
        #
        # Fetch market indicators from Polygon and other sources
        #
        # Initialize shared inputs
        polygon_client = PolygonAPI.new
        lnmarkets_client = LnMarketsAPI.new
        coinglass_client = CoinglassAPI.new
        t3_client = T3IndexAPI.new
  
        #
        # Fetch Open metrics
        #
  
        # Polygon inputs
        symbol_polygon = 'X:BTCUSD'
        timespan = 'minute'
        window = 60
        series_type = 'open'
  
        # RSI
        rsi_open = 0.0
        response_rsi = polygon_client.get_rsi(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_rsi[:status] == 'success' &&
          response_rsi[:body]['results']['values'].present?
          rsi_open = response_rsi[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          rsi_open = 0.0
        end
        # TODO - Fix rsi which is always returning 100
  
        # SMA
        simple_moving_average_open = 0.0
        response_sma = polygon_client.get_sma(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_sma[:status] == 'success' &&
          response_sma[:body]['results']['values'].present?
          simple_moving_average_open = response_sma[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          simple_moving_average_open = 0.0
        end
  
        # EMA
        exponential_moving_average_open = 0.0
        response_ema = polygon_client.get_ema(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, window, series_type)
        if response_ema[:status] == 'success' &&
          response_ema[:body]['results']['values'].present?
          exponential_moving_average_open = response_ema[:body]['results']['values'][0]['value'].to_f.round(2)
        else
          exponential_moving_average_open = 0.0
        end
  
        # MACD
        macd_histogram_open = 0.0
        short_window = 120
        long_window = 260
        signal_window = 30
        response_macd = polygon_client.get_macd(
          symbol_polygon, loop_start_timestamp_milliseconds, timespan, short_window, long_window, signal_window, series_type)
        if response_macd[:status] == 'success' &&
          response_macd[:body]['results']['values'].present?
          macd_histogram_open = response_macd[:body]['results']['values'][0]['histogram'].to_f.round(2)
        else
          macd_histogram_open = 0.0
        end
  
        # Volume
        volume_prev_interval = 0.0
        aggregates_timespan = 'hour'
        aggregates_multiplier = 1
        start_date = (loop_start_timestamp_milliseconds - 1.hour.to_i.in_milliseconds)
        end_date = loop_start_timestamp_milliseconds
        response_prev_volume = polygon_client.get_aggregate_bars(
          symbol_polygon, aggregates_timespan, aggregates_multiplier, start_date, end_date)
        if response_prev_volume[:status] == 'success' &&
          response_prev_volume[:body]['resultsCount'] > 0
          volume_prev_interval = response_prev_volume[:body]['results'][1]['v'].to_f.round(2)
        else
          volume_prev_interval = 0.0
        end
  
        # Candle Open
        candle_open = 0.0
        aggregates_timespan = 'hour'
        aggregates_multiplier = 1
        start_date = loop_start_timestamp_milliseconds
        end_date = (loop_start_timestamp_milliseconds + 1.hour.to_i.in_milliseconds)
        response_volume = polygon_client.get_aggregate_bars(
          symbol_polygon, aggregates_timespan, aggregates_multiplier, start_date, end_date)
        if response_volume[:status] == 'success' && response_volume[:body]['resultsCount'] > 0
          matching_result = response_volume[:body]['results'].find { |result| result['t'] == loop_start_timestamp_milliseconds }
  
          if matching_result
            candle_open = matching_result['o'].to_f.round(2)
          else
            # No matching result found
          end
        else
          # No results found or status is not 'success'
        end
  
        # Price BTCUSD Coinbase and Price BTCUSD Index
        price_btcusd_coinbase_open, price_btcusd_index_open = 0.0, 0.0
        #
        # Only fetch this data when script is being run within 10 minutes of interval start
        #
        if loop_start_timestamp_milliseconds == last_loop_start_timestamp_milliseconds &&
          minutes_since_loop_end_interval <= 10
          # Get Index
          lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
          if lnmarkets_response[:status] == 'success'
            price_btcusd_index = lnmarkets_response[:body]['index']
          else
            price_btcusd_index = 0.0
          end
          price_btcusd_index_open = price_btcusd_index.to_f.round(2)
  
          # Get Coinbase
          response_btc_usd_trades = polygon_client.get_trades(symbol_polygon)
          if response_macd[:status] == 'success'
            # Exchange id 1 is Coinbase
            # Exchange id 10 is Binance
            exchange_1_entry = response_btc_usd_trades[:body]["results"]
              .sort_by { |entry| -entry["participant_timestamp"].to_i }
              .find { |entry| entry["exchange"] == 1 && entry["conditions"].include?(2) }
  
            exchange_1_price = exchange_1_entry&.[]("price")
  
            if exchange_1_price == nil
              price_btcusd_coinbase_open = price_btcusd_index.to_f.round(2)
            else
              price_btcusd_coinbase_open = exchange_1_price.to_f.round(2)
            end
          end
        else
          puts 'Skip prices for Coinbase and Index since interval start timestamp not in the last 10 minutes.'
        end
  
        # Implied Volatility T3
        implied_volatility_t3_open = 0.0
        current_tick = Time.at(loop_start_timestamp_milliseconds / 1000).utc.strftime("%Y-%m-%d-%H-%M-%S")
        t3_response = t3_client.get_tick(current_tick)
        if t3_response[:status] == 'success'
          implied_volatility_t3_open = t3_response[:body]['value'].to_f.round(2)
        end
  
        #
        # CoinGlass Indicators - https://docs.coinglass.com/reference/version-10
        #
        symbol_coinglass = 'BTC'
        symbol_coinglass_long_short_ratio = 'BTCUSDT'
        start_timestamp_seconds = ((loop_start_timestamp_milliseconds - 1.hour.to_i.in_milliseconds) / 1000.0).round(0)
        end_timestamp_seconds = ((loop_start_timestamp_milliseconds) / 1000.0).round(0)
        interval = "1h"
        exchange = "Binance"
  
        # Avg Funding Rate
        avg_funding_rate_open = 0.0
        coinglass_response = coinglass_client.get_aggregated_funding_rates(
          symbol_coinglass, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success'
          avg_funding_rate_open = coinglass_response[:body]['data'][0]['c'].to_f.round(4)
        end
  
        # Aggregate Open Interest
        aggregate_open_interest_open = 0.0
        coinglass_response = coinglass_client.get_aggregated_open_interest(
          symbol_coinglass, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success' && 
          coinglass_response[:body]['data'].present?
          aggregate_open_interest_open = coinglass_response[:body]['data'][0]['c'].to_f.round(2)
        else
          aggregate_open_interest_open = 0.0
        end
  
        # Avg Long Short Ratio
        avg_long_short_ratio_open = 0.0
        coinglass_response = coinglass_client.get_accounts_long_short_ratio(
          exchange, symbol_coinglass_long_short_ratio, interval, start_timestamp_seconds, end_timestamp_seconds)
        if coinglass_response[:status] == 'success' &&
          coinglass_response[:body]['data'].present?
          avg_long_short_ratio_open = coinglass_response[:body]['data'][0]['longShortRatio'].to_f.round(4)
        else
          avg_long_short_ratio_open = 0.0
        end
  
        # Get the next ID
        next_id_query = "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`"
        max_retries = 3
        retries = 0
        begin
          next_id_result = bigquery.query next_id_query
        rescue Google::Cloud::Error, HTTPClient::ReceiveTimeoutError => e
          if retries < max_retries
            retries += 1
            sleep(2 ** retries) # Exponential backoff
            retry
          else
            raise e
          end
        end
        next_id = next_id_result.first[:next_id]
  
        #
        # Format timestamp
        #
        formatted_start_timestamp_milliseconds = Time.at(
          loop_start_timestamp_milliseconds / 1000.0).utc.strftime('%Y-%m-%d %H:%M:%S.%6N')
        formatted_end_timestamp_milliseconds = Time.at(
          (loop_start_timestamp_milliseconds + 1.hour.to_i.in_milliseconds) / 1000.0).utc.strftime('%Y-%m-%d %H:%M:%S.%6N')
  
        #
        # Prepare new data to insert to table
        #
        new_data = {
          id: next_id,
          timestamp_open: formatted_start_timestamp_milliseconds,
          timestamp_close: formatted_end_timestamp_milliseconds,
          rsi_open: rsi_open,
          volume_prev_interval: volume_prev_interval,
          simple_moving_average_open: simple_moving_average_open,
          exponential_moving_average_open: exponential_moving_average_open,
          macd_histogram_open: macd_histogram_open,
          candle_open: candle_open,
          price_btcusd_coinbase_open: price_btcusd_coinbase_open,
          price_btcusd_index_open: price_btcusd_index_open,
          avg_funding_rate_open: avg_funding_rate_open,
          aggregate_open_interest_open: aggregate_open_interest_open,
          implied_volatility_t3_open: implied_volatility_t3_open,
          avg_long_short_ratio_open: avg_long_short_ratio_open,
        }
        row = new_data
  
        #
        # Insert new data to table
        #
        dataset = bigquery.dataset(DATASET_ID)
        table = dataset.table(TABLE_ID)
  
        # Retry on select Google Cloud errors
        max_retries = 3
        retries = 0
        begin
          # Create a temporary file with JSON data
          temp_file = Tempfile.new(['bigquery_insert', '.json'])
          temp_file.write(JSON.generate(row))
          temp_file.close
  
          # Create a load job
          load_job = dataset.load_job TABLE_ID, temp_file.path, format: "json" do |job|
            job.autodetect = true
            job.write = "WRITE_APPEND"
          end
  
          puts "Starting job #{load_job.job_id}"
  
          # Wait for the job to complete
          load_job.wait_until_done!
  
          if load_job.failed?
            puts "Job failed with error: #{load_job.error}"
            raise load_job.error
          else
            puts "Row successfully inserted."
          end
  
        rescue => e
          if retries < max_retries
            retries += 1
            sleep(2 ** retries) # Exponential backoff
            retry
          else
            raise e
          end
        ensure
          # Make sure to delete the temporary file
          temp_file.unlink
        end
  
        # puts "Inserted new data: #{new_data}"
        loop_start_timestamp_milliseconds += 1.hour.to_i.in_milliseconds
        sleep(0.1)
      end
      puts "Loop finished before next interval: #{loop_start_timestamp_milliseconds}"
      puts "End operations:generate_hourly_training_data_next_interval"
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    end
  
    task generate_hourly_prediction: :environment do
      puts "Begin operations:generate_hourly_prediction"
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      #
      # Make prediction
      #
      require "google/cloud/bigquery"
  
      PROJECT_ID = "encrypted-energy"
      DATASET_ID = "market_indicators"
      TABLE_ID = "hourly_training_data"
      MODEL_ID = "hourly_random_forest"
  
      # Initialize BigQuery client
      bigquery = if defined?(Rails) && Rails.env.production?
        credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
        Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
      else
        Google::Cloud::Bigquery.new(project: PROJECT_ID)
      end
  
      # Get references to the dataset and table
      dataset = bigquery.dataset(DATASET_ID)
      table = dataset.table(TABLE_ID)
  
      # Query to get the latest data
      latest_data_query = <<-SQL
        SELECT
          id,
          rsi_open,
          volume_prev_interval,
          simple_moving_average_open,
          exponential_moving_average_open,
          macd_histogram_open,
          candle_open,
          price_btcusd_coinbase_open,
          price_btcusd_index_open,
          avg_funding_rate_open,
          aggregate_open_interest_open,
          implied_volatility_t3_open,
          avg_long_short_ratio_open
        FROM
          `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
        WHERE timestamp_close >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        ORDER BY id DESC
        LIMIT 1
      SQL
  
      # Execute the query to get the latest data
      latest_data = bigquery.query(latest_data_query).first
  
      if latest_data
        puts "Latest ID: #{latest_data[:id]}"
        # Query to make the prediction
        prediction_query = <<-SQL
          SELECT
            *
          FROM
            ML.PREDICT(MODEL `#{PROJECT_ID}.#{DATASET_ID}.#{MODEL_ID}`,
              (
                SELECT
                  CAST(#{latest_data[:rsi_open]} AS FLOAT64) AS rsi_open,
                  CAST(#{latest_data[:volume_prev_interval]} AS FLOAT64) AS volume_prev_interval,
                  CAST(#{latest_data[:simple_moving_average_open]} AS FLOAT64) AS simple_moving_average_open,
                  CAST(#{latest_data[:exponential_moving_average_open]} AS FLOAT64) AS exponential_moving_average_open,
                  CAST(#{latest_data[:macd_histogram_open]} AS FLOAT64) AS macd_histogram_open,
                  CAST(#{latest_data[:candle_open]} AS FLOAT64) AS candle_open,
                  CAST(#{latest_data[:price_btcusd_coinbase_open]} AS FLOAT64) AS price_btcusd_coinbase_open,
                  CAST(#{latest_data[:price_btcusd_index_open]} AS FLOAT64) AS price_btcusd_index_open,
                  CAST(#{latest_data[:avg_funding_rate_open]} AS FLOAT64) AS avg_funding_rate_open,
                  CAST(#{latest_data[:aggregate_open_interest_open]} AS FLOAT64) AS aggregate_open_interest_open,
                  CAST(#{latest_data[:implied_volatility_t3_open]} AS FLOAT64) AS implied_volatility_t3_open,
                  CAST(#{latest_data[:avg_long_short_ratio_open]} AS FLOAT64) AS avg_long_short_ratio_open
              )
            )
        SQL
  
        # Add debug logging
        puts "Prediction Query:"
        puts prediction_query
  
        # Execute the prediction query
        prediction_result = bigquery.query(prediction_query).first
  
        if prediction_result
          puts "Prediction Result Structure:"
          puts prediction_result.inspect
  
          # Extract the predicted direction and probabilities
          predicted_direction = prediction_result[:predicted_price_direction]
          probabilities = prediction_result[:predicted_price_direction_probs]
  
          puts "Predicted Direction: #{predicted_direction}"
          puts "Probabilities: #{probabilities.inspect}"
  
          up_probability = probabilities.find { |p| p[:label] == "up" }[:prob]
          down_probability = probabilities.find { |p| p[:label] == "down" }[:prob]
  
          # Prepare the row for update
          updated_row = {
            price_direction_prediction: predicted_direction,
            predicted_price_direction_probabilities: {
              up: up_probability,
              down: down_probability
            }
          }
  
          puts "Updated Row:"
          puts updated_row.inspect
  
          # Set up query parameters
          query_params = {
            id: latest_data[:id],
            price_direction_prediction: predicted_direction,
            up_probability: up_probability,
            down_probability: down_probability
          }
  
          # Construct the MERGE SQL statement
          update_set = "price_direction_prediction = @price_direction_prediction, " \
                       "predicted_price_direction_probabilities = STRUCT<up FLOAT64, down FLOAT64>(@up_probability, @down_probability)"
          merge_sql = <<-SQL
            MERGE `#{table.project_id}.#{table.dataset_id}.#{table.table_id}` T
            USING (SELECT @id AS id) S
            ON T.id = S.id
            WHEN MATCHED THEN
              UPDATE SET #{update_set}
          SQL
  
          # Execute the MERGE operation with retries
          max_retries = 3
          retries = 0
  
          begin
            job = dataset.query_job(
              merge_sql,
              params: query_params
            )
            job.wait_until_done!
  
            if job.failed?
              puts "Error updating row: #{job.error}"
              raise job.error
            else
              puts "Row updated successfully with prediction: #{predicted_direction}"
              puts "Up probability: #{up_probability}"
              puts "Down probability: #{down_probability}"
            end
          rescue Google::Cloud::Error, HTTPClient::ReceiveTimeoutError => e
            if retries < max_retries
              retries += 1
              sleep(2 ** retries) # Exponential backoff
              retry
            else
              raise e
            end
          end
        else
          puts "No prediction result"
        end
      else
        puts "No latest data found"
      end
       
      puts "End operations:generate_hourly_prediction"
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    end
  
    task update_hourly_model: :environment do
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts "Begin operations:update_hourly_model"
      #
      # Initialize BigQuery client
      #
      require "google/cloud/bigquery"
  
      PROJECT_ID = "encrypted-energy"
      DATASET_ID = "market_indicators"
  
      bigquery = if Rails.env.production?
        credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
        Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
      else
        Google::Cloud::Bigquery.new(project: PROJECT_ID)
      end
  
      # Step 1: Create or replace view
      VIEW_ID = "hourly_prepared_data"
      create_view_query = <<-SQL
        CREATE OR REPLACE VIEW `#{PROJECT_ID}.#{DATASET_ID}.#{VIEW_ID}` AS
        SELECT
          id,
          timestamp_open,
          rsi_open,
          volume_prev_interval,
          simple_moving_average_open,
          exponential_moving_average_open,
          macd_histogram_open,
          candle_open,
          price_btcusd_coinbase_open,
          price_btcusd_index_open,
          avg_funding_rate_open,
          aggregate_open_interest_open,
          implied_volatility_t3_open,
          avg_long_short_ratio_open,
          price_direction
        FROM
          `#{PROJECT_ID}.#{DATASET_ID}.hourly_training_data`
        WHERE
          timestamp_open >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
          AND rsi_open IS NOT NULL
          AND volume_prev_interval IS NOT NULL
          AND simple_moving_average_open IS NOT NULL
          AND exponential_moving_average_open IS NOT NULL
          AND macd_histogram_open IS NOT NULL
          AND candle_open IS NOT NULL
          AND price_btcusd_coinbase_open IS NOT NULL
          AND price_btcusd_index_open IS NOT NULL
          AND avg_funding_rate_open IS NOT NULL
          AND aggregate_open_interest_open IS NOT NULL
          AND implied_volatility_t3_open IS NOT NULL
          AND avg_long_short_ratio_open IS NOT NULL
          AND price_direction IS NOT NULL
      SQL
  
      # Execute the view creation query with wait and error handling
      begin
        view_job = bigquery.query_job create_view_query
        view_job.wait_until_done!
  
        if view_job.failed?
          puts "Error creating view: #{view_job.error}"
        else
          puts "View created successfully"
        end
      rescue StandardError => e
        puts "An error occurred while creating the view: #{e.message}"
      end
  
      # Step 2: Create or replace model
      MODEL_ID = 'hourly_random_forest'
      create_model_query = <<-SQL
        CREATE OR REPLACE MODEL `#{PROJECT_ID}.#{DATASET_ID}.#{MODEL_ID}`
        OPTIONS(
          model_type='RANDOM_FOREST_CLASSIFIER',
          input_label_cols=['price_direction'],
          data_split_method='RANDOM',
          data_split_eval_fraction=0.2,
          num_trials=10,
          max_tree_depth=10
        ) AS
        SELECT
          rsi_open,
          volume_prev_interval,
          simple_moving_average_open,
          exponential_moving_average_open,
          macd_histogram_open,
          candle_open,
          price_btcusd_coinbase_open,
          price_btcusd_index_open,
          avg_funding_rate_open,
          aggregate_open_interest_open,
          implied_volatility_t3_open,
          avg_long_short_ratio_open,
          price_direction
        FROM
          `#{PROJECT_ID}.#{DATASET_ID}.#{VIEW_ID}`
      SQL
  
      # Execute the model creation query with wait and error handling
      model_job = bigquery.query_job create_model_query
      model_job.wait_until_done!
  
      if model_job.error?
        puts "Error creating model: #{model_job.error}"
      else
        puts "Model created successfully"
      end
      #
      # For future consideration, additional tuning options:
      # max_parallel_trials=5,
      # max_trees=100,
      # min_tree_child_weight=1,
      # subsample=0.8,
      puts "End operations:update_hourly_model"
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
      puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    end
  end
