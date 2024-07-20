class CreateTradingStatsDaily < ActiveRecord::Migration[7.0]
  def change
    create_table :trading_stats_dailies do |t|
      t.datetime :recorded_date
      t.float :balance_btc
      t.float :balance_btc_sats
      t.float :balance_usd
      t.float :balance_usd_cents
      t.float :win_streak
      t.float :lose_streak
      t.float :last_100d_wins
      t.float :last_100d_losses
      t.datetime :created_at, default: ->{ 'now()' }
      t.datetime :updated_at, default: ->{ 'now()' }
    end
  end
end
