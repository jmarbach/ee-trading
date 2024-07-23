namespace :accountant do
  task save_trading_stats_daily: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run accountant:save_trading_stats_daily...'
    puts ''
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
        puts "Price BTCUSD: #{price_btcusd.to_fs(:delimited)}"

        price_sat_usd = (index_price_btcusd/100000000.0).round(5)
        balance_usd = (balance_btc_sats * price_sat_usd).round(2)
        balance_usd_cents = (balance_usd * 100.00).round(0)
        puts ""
        puts "Balance USD: #{balance_usd.to_fs(:delimited)}"

        #
        # Create new TradingStatsDaily record
        #
        trading_stats_daily = TradingStatsDaily.create(
          recorded_date: DateTime.now,
          balance_btc: balance_btc,
          balance_btc_sats: balance_btc_sats,
          balance_usd: balance_usd,
          balance_usd_cents: balance_usd_cents,
          win_streak: nil,
          lose_streak: nil,
          last_100d_wins: nil,
          last_100d_losses: nil
        )
      else
        puts 'Error. Unable to fetch latest price for BTCUSD.'
        abort 'Unable to proceed with saving USD attributes on TradingStatsDaily record.'
      end
    else
      puts 'Error. Unable to fetch latest account info from Lnmarkets.'
      abort 'Unable to proceed with creating TradingStatsDaily record.'
    end

    puts ''
    puts 'End accountant:save_trading_stats_daily...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end