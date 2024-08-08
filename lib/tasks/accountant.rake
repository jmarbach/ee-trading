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

        if previous_trading_stats_daily.win_streak != nil
          win_streak = previous_trading_stats_daily.win_streak
        else
          win_streak = 0
        end

        if previous_trading_stats_daily.lose_streak != nil
          lose_streak = previous_trading_stats_daily.lose_streak
        else
          lose_streak = 0
        end

        if previous_trading_stats_daily.draw_streak != nil
          draw_streak = previous_trading_stats_daily.draw_streak
        else
          draw_streak = 0
        end

        trade_result = ''
        if balance_btc > previous_trading_stats_daily.balance_btc
          trade_result = 'win'
        elsif balance_btc == previous_trading_stats_daily.balance_btc
          trade_result = 'draw'
        else
          trade_result = 'loss'
        end

        bool_win, bool_loss, bool_draw = false, false, false
        if trade_result == 'win'
          bool_win = true
          win_streak += 1
          lose_streak = 0
          draw_streak = 0
        elsif trade_result == 'loss'
          bool_loss = true
          win_streak = 0
          lose_streak += 1
          draw_streak = 0
        elsif trade_result == 'draw'
          bool_draw = true
          win_streak = 0
          lose_streak = 0
          draw_streak += 1
        end

        last_100d_wins, last_100d_losses, last_100d_draws = 0,0,0
        last_100d_records = TradingStatsDaily.order(recorded_date: :desc).limit(99)
        if last_100d_records.present? && !last_100d_records.empty?
          last_100d_wins = last_100d_records.where(win: true).count
          last_100d_losses = last_100d_records.where(loss: true).count
          last_100d_draws = last_100d_records.where(draw: true).count

          if trade_result == 'win'
            last_100d_wins += 1
          elsif trade_result == 'loss'
            last_100d_losses += 1
          elsif trade_result == 'draw'
            last_100d_draws += 1
          end
        end

        #
        # Net balance changes
        #
        net_balance_change_btc_absolute, net_balance_change_btc_sats_absolute = 0.0, 0.0
        net_balance_change_usd_absolute, net_balance_change_usd_cents_absolute = 0.0, 0.0
        net_balance_change_btc_percent, net_balance_change_usd_percent = 0.0, 0.0
        if previous_trading_stats_daily.balance_btc != nil
          net_balance_change_btc_absolute = (balance_btc - previous_trading_stats_daily.balance_btc)
          net_balance_change_btc_percent = (((balance_btc - previous_trading_stats_daily.balance_btc)/previous_trading_stats_daily.balance_btc)*100.0).round(2)
        end
        if previous_trading_stats_daily.balance_btc_sats != nil
          net_balance_change_btc_sats_absolute = (balance_btc_sats - previous_trading_stats_daily.balance_btc_sats)
        end
        if previous_trading_stats_daily.balance_usd != nil
          net_balance_change_usd_absolute = (balance_usd - previous_trading_stats_daily.balance_usd)
          net_balance_change_usd_percent = (((balance_usd - previous_trading_stats_daily.balance_usd)/previous_trading_stats_daily.balance_usd)*100.0).round(2)
        end
        if previous_trading_stats_daily.balance_usd_cents != nil
          net_balance_change_usd_cents_absolute = (balance_usd - previous_trading_stats_daily.balance_usd_cents)
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
          draw_streak: draw_streak,
          last_100d_wins: last_100d_wins,
          last_100d_losses: last_100d_losses,
          last_100d_draws: last_100d_draws,
          win: bool_win,
          loss: bool_loss,
          draw: bool_draw,
          net_balance_change_btc_absolute: net_balance_change_btc_absolute,
          net_balance_change_btc_sats_absolute: net_balance_change_btc_sats_absolute,
          net_balance_change_usd_absolute: net_balance_change_usd_absolute,
          net_balance_change_usd_cents_absolute: net_balance_change_usd_cents_absolute,
          net_balance_change_btc_percent: net_balance_change_btc_percent,
          net_balance_change_usd_percent: net_balance_change_usd_percent
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