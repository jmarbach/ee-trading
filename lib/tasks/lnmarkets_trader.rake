namespace :lnmarkets_trader do
  task close_all_positions: :environment do
    #
    # Close all existing futures and options contracts
    #
    lnmarkets_client = LnMarketsAPI.new
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
            script: "lnmarkets_trader:close_all_positions"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get running contracts.",
          script: "lnmarkets_trader:close_all_positions"
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
          trade_log = TradeLog.find_or_create_from_external_id(c, 'options')
          if trade_log.present?
            trade_log.update(
              running: false,
              closed: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.warn(
              {
                message: "Warning. Unable to find or create new TradeLog record for trade: #{c['id']}",
                script: "lnmarkets_trader:close_all_positions"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close open options contract: #{c['id']}",
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No running contracts.",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
    end

    puts ""
    puts "2. Check existing closed, open, and running futures trades..."
    puts "--------------------------------------------"
    #
    # Fetch futures created since yesterday beginning of day
    #
    timestamp_current = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    timestamp_yesterday = timestamp_current - 86400000
    closed_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('closed', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      closed_futures = lnmarkets_response[:body]
      if closed_futures.count > 0
        Rails.logger.info(
          {
            message: "Closed Futures: #{closed_futures.count}",
            script: "lnmarkets_trader:close_all_positions"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get closed futures trades.",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
    end

    if closed_futures.any?
      Rails.logger.info(
        {
          message: "Get trade stats from futures closed in prior trading interval...",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
      #
      # Cancel all open futures trades
      #
      closed_futures.each do |f|
        #
        # Update Trade Log
        #
        trade_log = TradeLog.find_or_create_from_external_id(f, 'futures')
        if trade_log.present?
          trade_log.update(
            open: false,
            running: false,
            closed: true,
            canceled: false
          )
          trade_log.get_final_trade_stats
        else
          Rails.logger.warn(
            {
              message: "Warning. Unable to find or create new TradeLog record for trade: #{f['id']}",
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No closed futures trades in prior interval.",
          script: "lnmarkets_trader:close_all_positions"
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
            script: "lnmarkets_trader:close_all_positions"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get open futures trades.",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
    end

    if open_futures.any?
      Rails.logger.info(
        {
          message: "Cancel all open futures trades from prior trading interval...",
          script: "lnmarkets_trader:close_all_positions"
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
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
          #
          # Update Trade Log
          #
          trade_log = TradeLog.find_or_create_from_external_id(f, 'futures')
          if trade_log.present?
            trade_log.update(
              open: false,
              canceled: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.warn(
              {
                message: "Warning. Unable to find or create new TradeLog record for trade: #{f['id']}",
                script: "lnmarkets_trader:close_all_positions"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close futures trade: #{f['id']}",
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No open futures trades.",
          script: "lnmarkets_trader:close_all_positions"
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
            script: "lnmarkets_trader:close_all_positions"
          }.to_json
        )
      end
    else
      Rails.logger.error(
        {
          message: "Error. Unable to get running futures trades.",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
    end

    if running_futures.any?
      Rails.logger.info(
        {
          message: "Close all running futures trades from prior trading interval...",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
      #
      # Close all running futures trades
      #
      running_futures.each do |f|
        lnmarkets_response = lnmarkets_client.close_futures_trade(f['id'])
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Finished closing futures trade: #{f['id']}.",
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
          #
          # Update Trade Log
          #
          trade_log = TradeLog.find_or_create_from_external_id(f, 'futures')
          if trade_log.present?
            trade_log.update(
              open: false,
              canceled: false,
              running: false,
              closed: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.warn(
              {
                message: "Warning. Unable to find or create new TradeLog record for trade: #{f['id']}",
                script: "lnmarkets_trader:close_all_positions"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to close futures trade: #{f['id']}",
              script: "lnmarkets_trader:close_all_positions"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No running futures trades.",
          script: "lnmarkets_trader:close_all_positions"
        }.to_json
      )
    end
  end

  task attempt_trade_daily_trend: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    timestamp_current = DateTime.now.utc.beginning_of_day.to_i.in_milliseconds
    timestamp_yesterday = timestamp_current - 86400000
    strategy = 'daily-trend'
    Rails.logger.info(
      {
        message: "Run lnmarkets_trader:attempt_trade_daily_trend...",
        body: "QUERY DATE: #{DateTime.now.utc.beginning_of_day} - #{timestamp_current}",
        script: "lnmarkets_trader:attempt_trade_daily_trend"
      }.to_json
    )

    #
    #
    # Initialize score
    #
    trade_direction_score = 0.0

    #
    # Query training data
    #
    Rails.logger.info(
      {
        message: "Query training data for latest prediction.",
        script: "lnmarkets_trader:attempt_trade_daily_trend"
      }.to_json
    )
    require "google/cloud/bigquery"

    PROJECT_ID = "encrypted-energy"
    DATASET_ID = "market_indicators"
    TABLE_ID = "daily_training_data"

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

    # Query to get the latest prediction
    latest_prediction_query = <<-SQL
      SELECT
        id,
        price_direction_prediction,
        predicted_price_direction_probabilities.up AS up_probability,
        predicted_price_direction_probabilities.down AS down_probability
      FROM
        `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
      WHERE
        price_direction_prediction IS NOT NULL
      AND
        DATE(timestamp_open) = DATE(CURRENT_TIMESTAMP(), 'UTC')
      ORDER BY id DESC
      LIMIT 1
    SQL

    # Execute the query to get the latest prediction
    latest_prediction = bigquery.query(latest_prediction_query).first

    if latest_prediction
      puts "Latest Prediction ID: #{latest_prediction[:id]}"
      puts "Predicted Direction: #{latest_prediction[:price_direction_prediction]}"
      puts "Up Probability: #{latest_prediction[:up_probability]}"
      puts "Down Probability: #{latest_prediction[:down_probability]}"

      # Calculate trade score
      if latest_prediction[:down_probability] > latest_prediction[:up_probability]
        trade_direction_score = -1 * latest_prediction[:down_probability]
      else
        trade_direction_score = latest_prediction[:up_probability]
      end

      # ToDo - Add cols for BQ project, dataset, table, and row id
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        strategy: strategy
      )

      puts "Calculated Trade Direction Score: #{trade_direction_score}"
    else
      puts "No prediction found"
    end

    #
    # Save ScoreLog
    #
    Rails.logger.info(
      {
        message: "Final Trade Direction Score: #{trade_direction_score}",
        script: "lnmarkets_trader:attempt_trade_daily_trend"
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
          script: "lnmarkets_trader:attempt_trade_daily_trend"
        }.to_json
      )
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

    puts ""
    puts "Evaluate if we should create a new trade..."
    puts "--------------------------------------------"
    puts ""
    #
    # Invoke trade order scripts
    #
    if trade_direction_score < -0.5 || trade_direction_score > 0.52
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
          script: "lnmarkets_trader:attempt_trade_daily_trend"
        }.to_json
      )

      if trade_direction == 'buy'
        Rake::Task["lnmarkets_trader:create_long_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      elsif trade_direction == 'sell'
        Rake::Task["lnmarkets_trader:create_short_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      end
      Rails.logger.info(
        {
          message: "Finished creating new #{trade_direction} trade.",
          script: "lnmarkets_trader:attempt_trade_daily_trend"
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
          script: "lnmarkets_trader:attempt_trade_daily_trend"
        }.to_json
      )
    end

    puts 'End lnmarkets_trader:attempt_trade_daily_trend...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task attempt_trade_hourly_trend: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'

    strategy = 'hourly-trend'
    #
    # Check for no open or running trades using this strategy
    #
    hourly_trend_trades_created_today = TradeLog.where(
      created_at: DateTime.now.utc.beginning_of_day.., strategy: strategy, open: [true, nil], running: [true, nil])

    if hourly_trend_trades_created_today.count > 0
      #
      # Iterate through each trade to get its open and running status
      #
      hourly_trend_trades_created_today.each do |t|
        lnmarkets_client = LnMarketsAPI.new
        lnmarkets_response = lnmarkets_client.get_futures_trade(t.external_id)
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Parse trade response from LnMarkets",
              body: "#{JSON.generate(lnmarkets_response[:body])}",
              script: "lnmarkets_trader:attempt_trade_hourly_trend"
            }.to_json
          )
          if lnmarkets_response[:body]['running'] == true
            Rails.logger.warn(
              {
                message: "We already opened an hourly trend trade that is still running. Skip.",
                script: "lnmarkets_trader:attempt_trade_hourly_trend"
              }.to_json
            )
            exit(0)
          elsif lnmarkets_response[:body]['open'] == true
            Rails.logger.warn(
              {
                message: "We already created an hourly trend trade that is still open. Cancel trade and continue.",
                script: "lnmarkets_trader:attempt_trade_hourly_trend"
              }.to_json
            )
            lnmarkets_response = lnmarkets_client.cancel_futures_trade(t.external_id)
            if lnmarkets_response[:status] == 'success'
              Rails.logger.info(
                {
                  message: "Finished closing open futures trade: #{t.external_id}.",
                  script: "lnmarkets_trader:attempt_trade_hourly_trend"
                }.to_json
              )
              TradeLog.find_by_external_id(t.external_id).update(
                open: false,
                canceled: true
              )
            else
              Rails.logger.error(
                {
                  message: "Error. Unable to close futures trade: #{t.external_id}",
                  script: "lnmarkets_trader:attempt_trade_hourly_trend"
                }.to_json
              )
            end
          else
            Rails.logger.info(
              {
                message: "Trade #{t.external_id} is not running. Update TradeLog record...",
                script: "lnmarkets_trader:attempt_trade_hourly_trend"
              }.to_json
            )
            t.update(
              open: false,
              running: false
            )
            # Proceed
          end
        else
          Rails.logger.fatal(
            {
              message: "Error. Unable to get futures trade.",
              script: "lnmarkets_trader:attempt_trade_hourly_trend"
            }.to_json
          )
          abort 'Unable to get state of futures trade.'
        end
      end
    end

    #
    #
    # Initialize score
    #
    trade_direction_score = 0.0

    #
    # Query training data
    #
    Rails.logger.info(
      {
        message: "Query training data for latest prediction.",
        script: "lnmarkets_trader:attempt_trade_hourly_trend"
      }.to_json
    )
    require "google/cloud/bigquery"

    PROJECT_ID = "encrypted-energy"
    DATASET_ID = "market_indicators"
    TABLE_ID = "hourly_training_data"

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

    # Query to get the latest prediction
    latest_prediction_query = <<-SQL
      SELECT
        id,
        price_direction_prediction,
        predicted_price_direction_probabilities.up AS up_probability,
        predicted_price_direction_probabilities.down AS down_probability
      FROM
        `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
      WHERE
        price_direction_prediction IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
    SQL

    # Execute the query to get the latest prediction
    latest_prediction = bigquery.query(latest_prediction_query).first

    if latest_prediction
      puts "Latest Prediction ID: #{latest_prediction[:id]}"
      puts "Predicted Direction: #{latest_prediction[:price_direction_prediction]}"
      puts "Up Probability: #{latest_prediction[:up_probability]}"
      puts "Down Probability: #{latest_prediction[:down_probability]}"

      # Calculate trade score
      if latest_prediction[:down_probability] > latest_prediction[:up_probability]
        trade_direction_score = -1 * latest_prediction[:down_probability]
      else
        trade_direction_score = latest_prediction[:up_probability]
      end

      # ToDo - Add cols for BQ project, dataset, table, and row id
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        strategy: strategy
      )

      puts "Calculated Trade Direction Score: #{trade_direction_score}"
    else
      puts "No prediction found"
    end

    #
    # Save ScoreLog
    #
    Rails.logger.info(
      {
        message: "Final Trade Direction Score: #{trade_direction_score}",
        script: "lnmarkets_trader:attempt_trade_hourly_trend"
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
          script: "lnmarkets_trader:attempt_trade_hourly_trend"
        }.to_json
      )
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

    puts ""
    puts "Evaluate if we should create a new trade..."
    puts "--------------------------------------------"
    puts ""
    #
    # Invoke trade order scripts
    #
    if trade_direction_score < -0.5 || trade_direction_score > 0.53
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
          script: "lnmarkets_trader:attempt_trade_hourly_trend"
        }.to_json
      )

      if trade_direction == 'buy'
        Rake::Task["lnmarkets_trader:create_long_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      elsif trade_direction == 'sell'
        Rake::Task["lnmarkets_trader:create_short_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      end
      Rails.logger.info(
        {
          message: "Finished creating new #{trade_direction} trade.",
          script: "lnmarkets_trader:attempt_trade_hourly_trend"
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
          script: "lnmarkets_trader:attempt_trade_hourly_trend"
        }.to_json
      )
    end

    puts 'End lnmarkets_trader:attempt_trade_hourly_trend...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task attempt_trade_thirty_minute_trend: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'

    strategy = 'thirty-minute-trend'
    #
    # Check for no open or running trades using this strategy
    #
    thirty_minute_trend_trades_created_today = TradeLog.where(
      created_at: DateTime.now.utc.beginning_of_day.., strategy: strategy, open: [true, nil], running: [true, nil])

    if thirty_minute_trend_trades_created_today.count > 0
      #
      # Iterate through each trade to get its open and running status
      #
      thirty_minute_trend_trades_created_today.each do |t|
        lnmarkets_client = LnMarketsAPI.new
        lnmarkets_response = lnmarkets_client.get_futures_trade(t.external_id)
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Parse trade response from LnMarkets",
              body: "#{JSON.generate(lnmarkets_response[:body])}",
              script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
            }.to_json
          )
          if lnmarkets_response[:body]['running'] == true
            Rails.logger.warn(
              {
                message: "We already opened an hourly trend trade that is still open or running. Skip.",
                script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
              }.to_json
            )
            exit(0)
          elsif lnmarkets_response[:body]['open'] == true
            Rails.logger.warn(
              {
                message: "We already created an hourly trend trade that is still open. Cancel trade and continue.",
                script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
              }.to_json
            )
            lnmarkets_response = lnmarkets_client.cancel_futures_trade(t.external_id)
            if lnmarkets_response[:status] == 'success'
              Rails.logger.info(
                {
                  message: "Finished closing open futures trade: #{t.external_id}.",
                  script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
                }.to_json
              )
              TradeLog.find_by_external_id(t.external_id).update(
                open: false,
                canceled: true
              )
            else
              Rails.logger.error(
                {
                  message: "Error. Unable to close futures trade: #{t.external_id}",
                  script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
                }.to_json
              )
            end
          else
            Rails.logger.info(
              {
                message: "Trade #{t.external_id} is not running or open. Update TradeLog record...",
                script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
              }.to_json
            )
            t.update(
              open: false,
              running: false
            )
            #
            # Proceed
            #
          end
        else
          Rails.logger.fatal(
            {
              message: "Error. Unable to get futures trade.",
              script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
            }.to_json
          )
          abort 'Unable to get state of futures trade.'
        end
      end
    end

    #
    #
    # Initialize score
    #
    trade_direction_score = 0.0

    #
    # Query training data
    #
    Rails.logger.info(
      {
        message: "Query training data for latest prediction.",
        script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
      }.to_json
    )
    require "google/cloud/bigquery"

    PROJECT_ID = "encrypted-energy"
    DATASET_ID = "market_indicators"
    TABLE_ID = "thirty_minute_training_data"

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

    # Query to get the latest prediction
    latest_prediction_query = <<-SQL
      SELECT
        id,
        price_direction_prediction,
        predicted_price_direction_probabilities.up AS up_probability,
        predicted_price_direction_probabilities.down AS down_probability
      FROM
        `#{PROJECT_ID}.#{DATASET_ID}.#{TABLE_ID}`
      WHERE
        price_direction_prediction IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
    SQL

    # Execute the query to get the latest prediction
    latest_prediction = bigquery.query(latest_prediction_query).first

    if latest_prediction
      puts "Latest Prediction ID: #{latest_prediction[:id]}"
      puts "Predicted Direction: #{latest_prediction[:price_direction_prediction]}"
      puts "Up Probability: #{latest_prediction[:up_probability]}"
      puts "Down Probability: #{latest_prediction[:down_probability]}"

      # Calculate trade score
      if latest_prediction[:down_probability] > latest_prediction[:up_probability]
        trade_direction_score = -1 * latest_prediction[:down_probability]
      else
        trade_direction_score = latest_prediction[:up_probability]
      end

      # ToDo - Add cols for BQ project, dataset, table, and row id
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        strategy: strategy
      )

      puts "Calculated Trade Direction Score: #{trade_direction_score}"
    else
      puts "No prediction found"
    end

    #
    # Save ScoreLog
    #
    Rails.logger.info(
      {
        message: "Final Trade Direction Score: #{trade_direction_score}",
        script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
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
          script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
        }.to_json
      )
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

    puts ""
    puts "Evaluate if we should create a new trade..."
    puts "--------------------------------------------"
    puts ""
    #
    # Invoke trade order scripts
    #
    if trade_direction_score < -0.5 || trade_direction_score > 0.53
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
          script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
        }.to_json
      )

      if trade_direction == 'buy'
        Rake::Task["lnmarkets_trader:create_long_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      elsif trade_direction == 'sell'
        Rake::Task["lnmarkets_trader:create_short_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      end
      Rails.logger.info(
        {
          message: "Finished creating new #{trade_direction} trade.",
          script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
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
          script: "lnmarkets_trader:attempt_trade_thirty_minute_trend"
        }.to_json
      )
    end

    puts 'End lnmarkets_trader:attempt_trade_thirty_minute_trend...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task check_three_minute_trend_indicators: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'

    strategy = 'three-minute-trend'
    #
    # Check for no open or running trades using this strategy
    #
    three_minute_trend_trades_created_today = TradeLog.where(
      created_at: DateTime.now.utc.beginning_of_day.., strategy: strategy, open: [true, nil], running: [true, nil])

    if three_minute_trend_trades_created_today.count > 0
      #
      # Iterate through each trade to get its open and running status
      #
      three_minute_trend_trades_created_today.each do |t|
        lnmarkets_client = LnMarketsAPI.new
        lnmarkets_response = lnmarkets_client.get_futures_trade(t.external_id)
        if lnmarkets_response[:status] == 'success'
          Rails.logger.info(
            {
              message: "Parse trade response from LnMarkets",
              body: "#{JSON.generate(lnmarkets_response[:body])}",
              script: "lnmarkets_trader:check_three_minute_trend_indicators"
            }.to_json
          )
          if lnmarkets_response[:body]['open'] == true ||
            lnmarkets_response[:body]['running'] == true
            Rails.logger.warn(
              {
                message: "We already opened a three minute trend trade that is still open or running. Skip.",
                script: "lnmarkets_trader:check_three_minute_trend_indicators"
              }.to_json
            )
            exit(0)
          else
            Rails.logger.info(
              {
                message: "Trade #{t.external_id} is not running or open. Update TradeLog record...",
                script: "lnmarkets_trader:check_three_minute_trend_indicators"
              }.to_json
            )
            t.update(
              open: false,
              running: false
            )
            # Proceed
          end
        else
          Rails.logger.fatal(
            {
              message: "Error. Unable to get futures trade.",
              script: "lnmarkets_trader:check_three_minute_trend_indicators"
            }.to_json
          )
          abort 'Unable to get state of futures trade.'
        end
      end
    end

    timestamp_current = (DateTime.now.utc - 1.minute).beginning_of_minute.to_i.in_milliseconds
    Rails.logger.info(
      {
        message: "Run lnmarkets_trader:check_three_minute_trend_indicators...",
        body: "QUERY DATE: #{DateTime.now.utc.beginning_of_day} - #{timestamp_current}",
        script: "lnmarkets_trader:check_three_minute_trend_indicators"
      }.to_json
    )

    # Standard model inputs
    polygon_client = PolygonAPI.new
    symbol = 'X:BTCUSD'
    timespan = 'minute'
    window = 3
    series_type = 'close'

    # Track data errors
    data_errors = 0

    # Get technical indicators from Polygon
    rsi_value = 0.0
    response_rsi = polygon_client.get_rsi(symbol, timestamp_current, timespan, window, series_type)

    if response_rsi[:status] == 'success'
      if response_rsi[:body]['results']['values'] != nil
        rsi_value = response_rsi[:body]['results']['values'][0]['value']
      else
        rsi = 0.0
      end
    else
      data_errors += 1
    end

    if rsi_value != 0.0
      Rails.logger.info(
        {
          message: "RSI Value",
          body: "#{rsi_value}",
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )
    elsif rsi_value == 0.0
      Rails.logger.fatal(
        {
          message: "RSI Value",
          body: "#{rsi_value}",
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )
      abort 'Unable to fetch last hour RSI Value.'
    end

    # Current BTCUSD price
    price_btcusd = 0.0
    currency_from = 'BTC'
    currency_to = 'USD'
    response_btcusd = polygon_client.get_last_trade(currency_from, currency_to)
    if response_btcusd[:status] == 'success'
      price_btcusd = response_btcusd[:body]['last']['price']
    else
      data_errors += 1
    end
    Rails.logger.info(
      {
        message: "Fetched Last BTCUSD Tick.",
        body: "#{price_btcusd}",
        script: "lnmarkets_trader:check_three_minute_trend_indicators"
      }.to_json
    )

    #
    # Save MarketDataLog
    #
    begin
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        price_btcusd: price_btcusd,
        rsi: rsi_value,
        strategy: strategy
      )
    rescue => e
      Rails.logger.error(
        {
          message: "Error. Unable to save market_data_log record.",
          body: "#{e}",
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )
      market_data_log = MarketDataLog.create(
        recorded_date: DateTime.now,
        strategy: strategy
      )
    end

    #
    # Initialize score
    #
    trade_direction_score = 0.0

    #
    # Evaluate rules
    #
    if rsi_value.present?
      if rsi_value > 85 && rsi_value < 88
        trade_direction_score += 1.0
      elsif rsi_value > 71 && rsi_value < 73
        trade_direction_score += 1.0
      elsif rsi_value < 11.6 && rsi_value > 10.5
        trade_direction_score -= 1.0
      end
    else
      data_errors += 1
    end

    #
    # Save ScoreLog
    #
    Rails.logger.info(
      {
        message: "Final Trade Direction Score: #{trade_direction_score}",
        script: "lnmarkets_trader:check_three_minute_trend_indicators"
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
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )
      score_log = ScoreLog.create(
        recorded_date: DateTime.now
      )
    end

    puts ""
    puts "Evaluate if we should create a new trade..."
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
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )

      if trade_direction == 'buy'
        Rake::Task["lnmarkets_trader:create_long_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      elsif trade_direction == 'sell'
        Rake::Task["lnmarkets_trader:create_short_trade"].execute({score_log_id: score_log.id, strategy: strategy})
      end
      Rails.logger.info(
        {
          message: "Finished creating new #{trade_direction} trade.",
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
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
          script: "lnmarkets_trader:check_three_minute_trend_indicators"
        }.to_json
      )
    end

    Rails.logger.info(
      {
        message: "Data Errors: #{data_errors}",
        script: "lnmarkets_trader:check_three_minute_trend_indicators"
      }.to_json
    )
    if data_errors > 0
      market_data_log.update(int_data_errors: data_errors)
    end

    puts 'End lnmarkets_trader:check_three_minute_trend_indicators...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end

  task :create_long_trade, [:score_log_id, :strategy] => :environment do |t, args|
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:create_long_trade...'
    puts ''
    Rails.logger.info(
      {
        message: "args[:score_log_id]: #{args[:score_log_id]}, args[:strategy]: #{args[:strategy]}",
        script: "lnmarkets_trader:create_long_trade"
      }.to_json
    )
    if !args[:score_log_id].present? || !args[:strategy].present?
      Rails.logger.fatal(
        {
          message: "Error. Invocation missing required params.",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )
      abort 'Unable to invoke create_long_trade script.'
    else
      score_log_id = args[:score_log_id]
      strategy = args[:strategy]
    end
    # Initialize lnmarkets_client
    lnmarkets_client = LnMarketsAPI.new

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
      # Define leverage factor using recent volatility data from the 'daily-trend' strategy
      #
      PROJECT_ID = 'encrypted-energy'
      bigquery = if defined?(Rails) && Rails.env.production?
        credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
        Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
      else
        Google::Cloud::Bigquery.new(project: PROJECT_ID)
      end

      # Query to get the last 16 days of volatility data
      volatility_query = <<-SQL
        SELECT implied_volatility_t3_open, macd_histogram_open
        FROM `encrypted-energy.market_indicators.daily_training_data`
        WHERE DATE(timestamp_close) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
        ORDER BY DATE(timestamp_close) DESC
        LIMIT 16
      SQL
      
      # Execute the query
      volatility_results = bigquery.query(volatility_query)
      
      # Extract the volatility values
      last_16_market_data_log_volatility_entries = volatility_results.map { |row| row[:implied_volatility_t3_close] }
      
      # Calculate the average
      if !last_16_market_data_log_volatility_entries.empty?
        last_16_implied_volatilities_t3_average = last_16_market_data_log_volatility_entries.compact.sum.fdiv(last_16_market_data_log_volatility_entries.compact.size).round(2)
      else
        last_16_implied_volatilities_t3_average = 0.0
      end

      if (volatility_results[0][:implied_volatility_t3_open] < (last_16_implied_volatilities_t3_average))
        leverage_factor = rand(3.1..3.4).round(1)
      else
        leverage_factor = rand(2.6..2.8)
      end
      Rails.logger.info(
        {
          message: "Leverage: #{leverage_factor}",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )

      #
      # Determine MACD
      #
      macd_value = 0.0
      last_macd_value = nil
      last_market_data_log_entry = volatility_results[0]
      if last_market_data_log_entry != nil
        if last_market_data_log_entry[:macd_histogram_open] != nil
          last_macd_value = last_market_data_log_entry[:macd_histogram_open]
        end
      end

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
      leverage = 4
      price = (bid_price_btcusd * 0.999925).round(0)
      quantity = capital_waged_usd
      takeprofit = (index_price_btcusd * 1.07).round(0)
      stoploss = (index_price_btcusd * 0.94).round(0)

      lnmarkets_response = lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "New Futures Trade Created",
            body: lnmarkets_response[:body]['id'],
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
          margin_percent_of_quantity: margin_percent_of_quantity,
          strategy: strategy
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        if strategy == 'daily-trend' && last_macd_value != nil && last_macd_value < -300
          Rails.logger.info(
            {
              message: "Open directional hedge. Last MACD: #{last_macd_value}",
              script: "lnmarkets_trader:create_long_trade"
            }.to_json
          )
          Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'short', amount: quantity, score_log_id: score_log_id, strategy: strategy})
        end
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

  task :create_short_trade, [:score_log_id, :strategy] => :environment do |t, args|
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run lnmarkets_trader:create_short_trade...'
    puts ''
    puts 
    Rails.logger.info(
      {
        message: "args[:score_log_id]: #{args[:score_log_id]}, args[:strategy]: #{args[:strategy]}",
        script: "lnmarkets_trader:create_short_trade"
      }.to_json
    )
    if !args[:score_log_id].present? || !args[:strategy].present?
      Rails.logger.fatal(
        {
          message: "Error. Invocation missing required params.",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )
      abort 'Unable to invoke create_short_trade script.'
    else
      score_log_id = args[:score_log_id]
      strategy = args[:strategy]
    end
    # Initialize lnmarkets_client
    lnmarkets_client = LnMarketsAPI.new

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
      PROJECT_ID = 'encrypted-energy'
      bigquery = if defined?(Rails) && Rails.env.production?
        credentials = JSON.parse(ENV['GOOGLE_APPLICATION_CREDENTIALS'], symbolize_names: true)
        Google::Cloud::Bigquery.new(credentials: credentials, project: PROJECT_ID)
      else
        Google::Cloud::Bigquery.new(project: PROJECT_ID)
      end

      Rails.logger.info(
        {
          message: "Query latest volatility data...",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )
      
      # Query to get the last 16 days of volatility data
      volatility_query = <<-SQL
        SELECT implied_volatility_t3_open, macd_histogram_open
        FROM `encrypted-energy.market_indicators.daily_training_data`
        WHERE DATE(timestamp_close) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
        ORDER BY DATE(timestamp_close) DESC
        LIMIT 16
      SQL
      
      # Execute the query
      volatility_results = bigquery.query(volatility_query)
      
      # Extract the volatility values
      last_16_market_data_log_volatility_entries = volatility_results.map { |row| row[:implied_volatility_t3_close] }
      
      # Calculate the average
      if !last_16_market_data_log_volatility_entries.empty?
        last_16_implied_volatilities_t3_average = last_16_market_data_log_volatility_entries.compact.sum.fdiv(last_16_market_data_log_volatility_entries.compact.size).round(2)
      else
        last_16_implied_volatilities_t3_average = 0.0
      end
      Rails.logger.info(
        {
          message: "last_16_implied_volatilities_t3_average: #{last_16_implied_volatilities_t3_average}",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )

      if last_16_implied_volatilities_t3_average != 0.0 &&
        (volatility_results[0][:implied_volatility_t3_open] < (last_16_implied_volatilities_t3_average))
        leverage_factor = rand(3.1..3.4).round(1)
      else
        leverage_factor = rand(2.6..2.8)
      end
      Rails.logger.info(
        {
          message: "Leverage: #{leverage_factor}",
          script: "lnmarkets_trader:create_long_trade"
        }.to_json
      )

      #
      # Determine MACD
      #
      macd_value = 0.0
      last_macd_value = nil
      last_market_data_log_entry = volatility_results[0]
      if last_market_data_log_entry != nil
        if last_market_data_log_entry[:macd_histogram_open] != nil
          last_macd_value = last_market_data_log_entry[:macd_histogram_open]
        end
      end

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
      leverage = 4
      price = (ask_price_btcusd * 1.000025).round(0)
      quantity = capital_waged_usd
      takeprofit = (index_price_btcusd * 0.93).round(0)
      stoploss = (index_price_btcusd * 1.06).round(0)

      Rails.logger.info(
        {
          message: "Attempt to create new futures trade. Current timestamp: #{DateTime.now.utc.to_i.in_milliseconds}",
          script: "lnmarkets_trader:create_short_trade"
        }.to_json
      )

      lnmarkets_response = lnmarkets_client.create_futures_trades(side, type, leverage, price, quantity, takeprofit, stoploss)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "New Futures Trade Created",
            body: lnmarkets_response[:body]['id'],
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
          margin_percent_of_quantity: margin_percent_of_quantity,
          strategy: strategy
        )
        #
        # Open directional hedge by buying options contract in the inverse direction
        #
        if strategy == 'daily-trend' && last_macd_value != nil && last_macd_value < -300
          Rails.logger.info(
            {
              message: "Open directional hedge. Last MACD: #{last_macd_value}",
              script: "lnmarkets_trader:create_long_trade"
            }.to_json
          )
          Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'long', amount: quantity, score_log_id: score_log_id, strategy: strategy})
        end
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

  task :open_options_contract, [:direction, :amount, :score_log_id, :strategy] => :environment do |t, args|
    #
    # Invoke this script with the following command:
    # Rake::Task["lnmarkets_trader:open_options_contract"].execute({direction: 'long', amount: lnmarkets_response[:body]['quantity'], score_log_id: score_log_id, strategy: strategy})
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
    puts "args[:score_log_id]: #{args[:strategy]}"
    if args[:direction].present? && 
      args[:amount].present? &&
      args[:score_log_id].present? &&
      args[:strategy].present?

      #
      # Evaluate direction argument
      #
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

      #
      # Assign capital waged based on Lnmarkets trading limits
      #
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

      #
      # Initialize score_log_id var
      #
      score_log_id = args[:score_log_id]

      #
      # Evaluate strategy argument
      #
      if ['daily-trend'].include?(args[:strategy])
        strategy = args[:strategy]
      else
        Rails.logger.fatal(
          {
            message: "Error. Invalid strategy parameter.",
            script: "lnmarkets_trader:open_options_contract"
          }.to_json
        )
        abort 'Unable to invoke open_options_contract script.'
      end

      # Initialize lnmarkets_client
      lnmarkets_client = LnMarketsAPI.new

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
          all_instruments = lnmarkets_response[:body]
          filtered_instruments = all_instruments.select {|y| y.include?((DateTime.now + 1.day).utc.strftime("BTC.%Y-%m-%d")) }

          if direction == 'long'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.C') }
            price_levels = [index_price_btcusd - 1000, index_price_btcusd - 2000, index_price_btcusd - 3000]

            price_levels.each do |price|
              temp_filtered = filtered_instruments.select { |y| y.include?(price.ceil(-3).to_s) }
              if temp_filtered.any?
                filtered_instruments = temp_filtered
                break
              end
            end
          elsif direction == 'short'
            filtered_instruments = filtered_instruments.select { |y| y.include?('.P') }
            price_levels = [index_price_btcusd, index_price_btcusd + 1000, index_price_btcusd + 2000]

            price_levels.each do |price|
              temp_filtered = filtered_instruments.select { |y| y.include?(price.ceil(-3).to_s) }
              if temp_filtered.any?
                filtered_instruments = temp_filtered
                break
              end
            end
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
              body: lnmarkets_response[:body]['id'],
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
            score_log_id: score_log_id,
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
            strike: lnmarkets_response[:body]['strike'],
            instrument: instrument_name,
            settlement: settlement,
            implied_volatility: lnmarkets_response[:body]['volatility'],
            running: true,
            closed: false,
            margin_percent_of_quantity: margin_percent_of_quantity,
            strategy: strategy
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
    lnmarkets_client = LnMarketsAPI.new

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
      index_price_btcusd, ask_price_btcusd, bid_price_btcusd = 0.0, 0.0, 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        ask_price_btcusd = lnmarkets_response[:body]['askPrice']
        bid_price_btcusd = lnmarkets_response[:body]['bidPrice']
        Rails.logger.info(
          {
            message: "INDEX: #{index_price_btcusd.to_fs(:delimited)} | ASK: #{ask_price_btcusd.to_fs(:delimited)} | BID: #{bid_price_btcusd.to_fs(:delimited)}",
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
        # Get strategy of trade from EE database
        #
        trade_log = TradeLog.find_or_create_from_external_id(f, 'futures')
        if trade_log.present?
          strategy = trade_log.strategy
          Rails.logger.info(
            {
              message: "Futures Trade Strategy: #{strategy}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        else
          Rails.logger.warn(
            {
              message: "Warning. Unable to find or create new TradeLog record for trade: #{f['id']}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end

        #
        # Get trade direction, long/short
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
        # Get trade entry price and previous stoploss/takeprofits
        #
        entry_price = f['entry_price']
        previous_stoploss = f['stoploss']
        previous_takeprofit = f['takeprofit']
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

        #
        # Check if position is 'In-the-Money'
        #
        update_trade_stoploss_price = false
        if trade_direction == 'long'
          if bid_price_btcusd > entry_price
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Update stop-loss for long position, #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            update_trade_stoploss_price = true
          else
            #
            # Do not update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Position is not In-the-Money. Do not update stop-loss. Position: #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            next
          end
        elsif trade_direction == 'short'
          if ask_price_btcusd < entry_price
            #
            # Update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Update stop-loss for short position, #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            update_trade_stoploss_price = true
          else
            #
            # Do not update the position's stop-loss
            #
            Rails.logger.info(
              {
                message: "Position is not In-the-Money. Do not update stop-loss. Position: #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            next
          end
        end

        #
        # 4. Update the position's stop-loss based on the TradeLog's strategy
        #
        if update_trade_stoploss_price == true
          Rails.logger.info(
            {
              message: "Attempt to update futures trade #{f['id']} with the #{strategy} strategy...",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
          #
          # Calculate new stoploss
          #
          if strategy == 'daily-trend'
            if trade_direction == 'long'
              if bid_price_btcusd > (entry_price * 1.035)
                new_stoploss = (entry_price * 1.035).round(0)
              elsif bid_price_btcusd > (entry_price * 1.026)
                new_stoploss = (entry_price * 1.026).round(0)
              elsif bid_price_btcusd > (entry_price * 1.025)
                new_stoploss = (entry_price * 1.025).round(0)
              elsif bid_price_btcusd > (entry_price * 1.02)
                new_stoploss = (entry_price * 1.01).round(0)
              elsif bid_price_btcusd > (entry_price * 1.015)
                new_stoploss = (entry_price * 1.0).round(0)
              elsif bid_price_btcusd > (entry_price * 1.01)
                new_stoploss = (entry_price * 0.99).round(0)
              elsif bid_price_btcusd > (entry_price * 1.005)
                new_stoploss = (entry_price * 0.95).round(0)
              else
                new_stoploss = (bid_price_btcusd * 0.94).round(0)
              end
            elsif trade_direction == 'short'
              if ask_price_btcusd < (entry_price * 0.965)
                new_stoploss = (entry_price * 0.965).round(0)
              elsif ask_price_btcusd < (entry_price * 0.974)
                new_stoploss = (entry_price * 0.974).round(0)
              elsif ask_price_btcusd < (entry_price * 0.975)
                new_stoploss = (entry_price * 0.975).round(0)
              elsif ask_price_btcusd < (entry_price * 0.98)
                new_stoploss = (entry_price * 0.99).round(0)
              elsif ask_price_btcusd < (entry_price * 0.985)
                new_stoploss = (entry_price * 1.0).round(0)
              elsif ask_price_btcusd < (entry_price * 0.99)
                new_stoploss = (entry_price * 1.01).round(0)
              elsif ask_price_btcusd < (entry_price * 0.995)
                new_stoploss = (entry_price * 1.05).round(0)
              else
                new_stoploss = (ask_price_btcusd * 1.06).round(0)
              end
            end
          elsif strategy == 'hourly-trend'
            if trade_direction == 'long'
              if bid_price_btcusd > (entry_price * 1.025)
                new_stoploss = (entry_price * 1.025).round(0)
              elsif bid_price_btcusd > (entry_price * 1.02)
                new_stoploss = (entry_price * 1.02).round(0)
              elsif bid_price_btcusd > (entry_price * 1.015)
                new_stoploss = (entry_price * 1.015).round(0)
              elsif bid_price_btcusd > (entry_price * 1.01)
                new_stoploss = (entry_price * 1.01).round(0)
              elsif bid_price_btcusd > (entry_price * 1.005)
                new_stoploss = (entry_price * 1.005).round(0)
              else
                new_stoploss = (bid_price_btcusd * 0.94).round(0)
              end
            elsif trade_direction == 'short'
              if ask_price_btcusd < (entry_price * 0.975)
                new_stoploss = (entry_price * 0.975).round(0)
              elsif ask_price_btcusd < (entry_price * 0.98)
                new_stoploss = (entry_price * 0.98).round(0)
              elsif ask_price_btcusd < (entry_price * 0.985)
                new_stoploss = (entry_price * 0.985).round(0)
              elsif ask_price_btcusd < (entry_price * 0.99)
                new_stoploss = (entry_price * 0.99).round(0)
              elsif ask_price_btcusd < (entry_price * 0.995)
                new_stoploss = (entry_price * 0.995).round(0)
              else
                new_stoploss = (ask_price_btcusd * 1.06).round(0)
              end
            end
          elsif ['three-minute-trend','unknown','thirty-minute-trend'].include?(strategy)
            if trade_direction == 'long'
              if bid_price_btcusd > (entry_price * 1.0135)
                new_stoploss = (entry_price * 1.0135).round(0)
              elsif bid_price_btcusd > (entry_price * 1.0125)
                new_stoploss = (entry_price * 1.0125).round(0)
              elsif bid_price_btcusd > (entry_price * 1.01)
                new_stoploss = (entry_price * 1.01).round(0)
              elsif bid_price_btcusd > (entry_price * 1.0075)
                new_stoploss = (entry_price * 1.0075).round(0)
              elsif bid_price_btcusd > (entry_price * 1.0060)
                new_stoploss = (entry_price * 1.0060).round(0)
              elsif bid_price_btcusd > (entry_price * 1.005)
                new_stoploss = (entry_price * 1.005).round(0)
              elsif bid_price_btcusd > (entry_price * 1.0032)
                new_stoploss = (entry_price * 1.0032).round(0)
              elsif bid_price_btcusd > (entry_price * 1.00219)
                new_stoploss = (entry_price * 1.00219).round(0)
              elsif bid_price_btcusd > (entry_price * 1.0016)
                new_stoploss = (entry_price * 0.99).round(0)
              else
                #
                # Check for liquidation levels edge case for 'unknown' trades
                #
                if f['liquidation'] > (bid_price_btcusd * 0.94).round(0)
                  new_stoploss = (f['liquidation'] + 1.00).round(0)
                else
                  new_stoploss = (bid_price_btcusd * 0.94).round(0)
                end
              end
            elsif trade_direction == 'short'
              if ask_price_btcusd < (entry_price * 0.9865)
                new_stoploss = (entry_price * 0.9865).round(0)
              elsif ask_price_btcusd < (entry_price * 0.9875)
                new_stoploss = (entry_price * 0.9875).round(0)
              elsif ask_price_btcusd < (entry_price * 0.99)
                new_stoploss = (entry_price * 0.99).round(0)
              elsif ask_price_btcusd < (entry_price * 0.9925)
                new_stoploss = (entry_price * 0.9925).round(0)
              elsif ask_price_btcusd < (entry_price * 0.994)
                new_stoploss = (entry_price * 0.994).round(0)
              elsif ask_price_btcusd < (entry_price * 0.995)
                new_stoploss = (entry_price * 0.995).round(0)
              elsif ask_price_btcusd < (entry_price * 0.9968)
                new_stoploss = (entry_price * 0.9968).round(0)
              elsif ask_price_btcusd < (entry_price * 0.9781)
                new_stoploss = (entry_price * 0.9781).round(0)
              elsif ask_price_btcusd < (entry_price * 0.9984)
                new_stoploss = (entry_price * 1.01).round(0)
              else
                #
                # Check for liquidation levels edge case for 'unknown' trades
                #
                if f['liquidation'] < (ask_price_btcusd * 1.06).round(0)
                  new_stoploss = (f['liquidation'] - 1.00).round(0)
                else
                  new_stoploss = (ask_price_btcusd * 1.06).round(0)
                end
              end
            end
          end
          #
          # Log new stoploss
          #
          Rails.logger.info(
            {
              message: "New stoploss: #{new_stoploss.to_fs(:delimited)}  (Previously: #{previous_stoploss})",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )

          #
          # Only proceed with update to LnMarkets if the stoploss value is not the same as the old value
          #
          if new_stoploss != previous_stoploss
            lnmarkets_response = lnmarkets_client.update_futures_trade(f['id'], 'stoploss', new_stoploss)
            if lnmarkets_response[:status] == 'success'
              Rails.logger.info(
                {
                  message: "Updated stoploss for #{f['id']}:",
                  body: lnmarkets_response[:body]['stoploss'],
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
              #
              # Then if stoploss was updated, update takeprofit for 'three-minute' and 'unknown' trades
              #
              if ['three-minute-trend', 'unknown'].include?(strategy)
                if trade_direction == 'long'
                  new_takeprofit = (entry_price * 1.015).round(0)
                elsif trade_direction == 'short'
                  new_takeprofit = (entry_price * 0.985).round(0)
                end

                if new_takeprofit != previous_takeprofit
                  # Send update to LnMarkets
                  lnmarkets_response = lnmarkets_client.update_futures_trade(f['id'], 'takeprofit', new_takeprofit)

                  if lnmarkets_response[:status] == 'success'
                    Rails.logger.info(
                      {
                        message: "Updated takeprofit for #{f['id']}:",
                        body: lnmarkets_response[:body]['takeprofit'],
                        script: "lnmarkets_trader:check_stops"
                      }.to_json
                    )
                  else
                    Rails.logger.error(
                      {
                        message: "Error. Unable to update takeprofit for futures trade, #{f['id']}.",
                        script: "lnmarkets_trader:check_stops"
                      }.to_json
                    )
                  end
                else
                  Rails.logger.warn(
                    {
                      message: "Warning. New takeprofit is equal to previous takeprofit. Skipped updating futures trade.",
                      script: "lnmarkets_trader:check_stops"
                    }.to_json
                  )
                end
              end
            else
              Rails.logger.error(
                {
                  message: "Error. Unable to update stoploss for futures trade, #{f['id']}.",
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
              message: "Futures Position is not In-the-Money. Do no update stoploss.",
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
        puts ''
        puts '---------------------------------------------------'
        puts '---------------------------------------------------'
        puts ''
        Rails.logger.info(
          {
            message: "Options Contract ID: #{c['id']}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
        #
        # Get trade direction, long/short
        #
        trade_direction = ''
        if c['type'] == 'c'
          trade_direction = 'long'
        elsif c['type'] == 'p'
          trade_direction = 'short'
        end

        #
        # Get trade entry price if f['side'] == 'b'
        #
        entry_price = c['forward']
        Rails.logger.info(
          {
            message: "Options Trade Entry Price: #{entry_price.to_fs(:delimited)}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )

        #
        # Get strategy of trade from EE database
        #
        trade_log = TradeLog.find_or_create_from_external_id(c, 'options')
        if trade_log.present?
          strategy = trade_log.strategy
          Rails.logger.info(
            {
              message: "Options Trade Strategy: #{strategy}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        else
          Rails.logger.warn(
            {
              message: "Warning. Unable to find or create new TradeLog record for trade: #{c['id']}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end

        #
        # Check if position is 'In-the-Money'
        #
        close_running_contract = false
        if strategy == 'daily-trend'
          if trade_direction == 'long'
            if index_price_btcusd > (entry_price * 1.0595)
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
              Rails.logger.info(
                {
                  message: "Options position requires no changes.",
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
              next
            end
          elsif trade_direction == 'short'
            if index_price_btcusd < (entry_price * 0.9405)
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
              Rails.logger.info(
                {
                  message: "Options position requires no changes.",
                  script: "lnmarkets_trader:check_stops"
                }.to_json
              )
              next
            end
          end
        else
          Rails.logger.info(
            {
              message: "Options position is running an unknown strategy. No changes.",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
          next
        end

        if close_running_contract == true
          Rails.logger.info(
            {
              message: "Attempt to close options contract #{c['id']} opened under the #{strategy} strategy...",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
          lnmarkets_response = lnmarkets_client.close_options_contract(c['id'])

          if lnmarkets_response[:status] == 'success'
            Rails.logger.info(
              {
                message: "Finished closing open options contract: #{c['id']}.",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            #
            # Update Trade Log
            #
            trade_log.update(
              running: false,
              closed: true
            )
            trade_log.get_final_trade_stats
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
      puts ''
      puts '---------------------------------------------------'
      puts '---------------------------------------------------'
      puts ''
    else
      Rails.logger.info(
        {
          message: "Skip. No running options contracts.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    #
    # 3. Check for any open futures positions...
    #
    open_futures = []
    lnmarkets_response = lnmarkets_client.get_futures_trades('open', timestamp_yesterday, timestamp_current)
    if lnmarkets_response[:status] == 'success'
      open_futures = lnmarkets_response[:body]
      if open_futures.count > 0
        Rails.logger.info(
          {
            message: "Open Futures: #{open_futures.count}",
            script: "lnmarkets_trader:check_stops"
          }.to_json
        )
      end
    else
      Rails.logger.fatal(
        {
          message: "Error. Unable to get open futures trades.",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )
    end

    if open_futures.any?
      Rails.logger.info(
        {
          message: "For trades not on 'daily-trend' or 'thirty-minute-trend', cancel all open futures trades from prior trading interval...",
          script: "lnmarkets_trader:check_stops"
        }.to_json
      )

      #
      # Evaluate trade's trend and cancel open futures trade if neceessary
      #
      open_futures.each do |f|
        #
        # Find Trade Log
        #
        trade_log = TradeLog.find_or_create_from_external_id(f, 'futures')
        if trade_log.present?
          strategy = trade_log.strategy
          Rails.logger.info(
            {
              message: "Futures Trade Strategy: #{strategy}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        else
          Rails.logger.warn(
            {
              message: "Warning. Unable to find or create new TradeLog record for trade: #{f['id']}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end

        #
        # Only cancel trades that are not part of the 'daily-trend' or 'thirty-minute' strategy
        #
        if trade_log.present? && !['daily-trend', 'thirty-minute-trend'].include?(trade_log.strategy)
          lnmarkets_response = lnmarkets_client.cancel_futures_trade(f['id'])
          if lnmarkets_response[:status] == 'success'
            Rails.logger.info(
              {
                message: "Finished closing open futures trade: #{f['id']}.",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
            trade_log.update(
              open: false,
              canceled: true
            )
            trade_log.get_final_trade_stats
          else
            Rails.logger.error(
              {
                message: "Error. Unable to close futures trade: #{f['id']}",
                script: "lnmarkets_trader:check_stops"
              }.to_json
            )
          end
        else
          Rails.logger.error(
            {
              message: "Error. Unable to find trade log for trade: #{f['id']}",
              script: "lnmarkets_trader:check_stops"
            }.to_json
          )
        end
      end
    else
      Rails.logger.info(
        {
          message: "Skip. No open futures trades.",
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
