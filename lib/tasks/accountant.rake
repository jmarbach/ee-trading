namespace :accountant do
  task save_trading_stats_daily: :environment do
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts ''
    puts 'Run accountant:save_trading_stats_daily...'
    puts ''
    #  id                :bigint           not null, primary key
    #  recorded_date     :datetime
    #  balance_btc       :float
    #  balance_btc_sats  :float
    #  balance_usd       :float
    #  balance_usd_cents :float
    #  win_streak        :float
    #  lose_streak       :float
    #  last_100d_wins    :float
    #  last_100d_losses  :float
    #  created_at        :datetime
    #  updated_at        :datetime
    puts ''
    puts 'End accountant:save_trading_stats_daily...'
    puts ''
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
    puts '/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/'
  end
end