namespace :accountant do
  task save_trading_stats_daily: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    Rails.logger.info(
      {
        message: "Run accountant:save_trading_stats_daily...",
        script: "accountant:save_trading_stats_daily"
      }.to_json
    )
    # Initialize lnmarkets_client
    lnmarkets_client = LnmarketsAPI.new

    #
    # Establish balance available to trade
    #
    lnmarkets_response = lnmarkets_client.get_user_info
    if lnmarkets_response[:status] == 'success'
      balance_btc_sats = lnmarkets_response[:body]['balance'].to_f.round(2)
      balance_btc = (balance_btc_sats/100000000.00).round(8)

      #
      # Fetch latest price of BTCUSD
      #
      index_price_btcusd, ask_price_btcusd, bid_price_btcusd = 0.0, 0.0, 0.0
      lnmarkets_response = lnmarkets_client.get_price_btcusd_ticker
      if lnmarkets_response[:status] == 'success'
        index_price_btcusd = lnmarkets_response[:body]['index']
        ask_price_btcusd = lnmarkets_response[:body]['askPrice']
        bid_price_btcusd = lnmarkets_response[:body]['askPrice']
        puts "Price BTCUSD: #{index_price_btcusd.to_fs(:delimited)}"

        price_sat_usd = (index_price_btcusd/100000000.0).round(5)
        balance_usd = (balance_btc_sats * price_sat_usd).round(2)
        balance_usd_cents = (balance_usd * 100.00).round(0)
        puts ""
        puts "Balance USD: #{balance_usd.to_fs(:delimited)}"

        #
        # Fetch last TradingStatsDaily record to establish streaks
        #
        previous_trading_stats_daily = TradingStatsDaily.last

        win_streak = previous_trading_stats_daily.win_streak
        lose_streak = previous_trading_stats_daily.lose_streak

        trade_result = ''
        if balance_btc > previous_trading_stats_daily.balance_btc
          trade_result = 'win'
        else
          trade_result = 'loss'
        end

        if trade_result == 'win'
          win_streak += 1
          lose_streak = 0
        elsif trade_result = 'loss'
          win_streak = 0
          lose_streak += 1
        end

        #
        # Create new TradingStatsDaily record
        #
        trading_stats_daily = TradingStatsDaily.create(
          recorded_date: DateTime.now,
          balance_btc: balance_btc,
          balance_btc_sats: balance_btc_sats,
          balance_usd: balance_usd,
          balance_usd_cents: balance_usd_cents,
          win_streak: win_streak,
          lose_streak: lose_streak,
          last_100d_wins: nil,
          last_100d_losses: nil
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to fetch latest price for BTCUSD.",
            script: "accountant:save_trading_stats_daily"
          }.to_json
        )
        abort 'Unable to proceed with saving USD attributes on TradingStatsDaily record.'
      end
    else
      Rails.logger.fatal(
        {
          message: "Error. Unable to fetch latest account info from Lnmarkets.",
          script: "accountant:save_trading_stats_daily"
        }.to_json
      )
      abort 'Unable to proceed with creating TradingStatsDaily record.'
    end

    Rails.logger.info(
      {
        message: "End accountant:save_trading_stats_daily...",
        script: "accountant:save_trading_stats_daily"
      }.to_json
    )
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end