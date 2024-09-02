class CreateTradingStatsYearlies < ActiveRecord::Migration[7.0]
  def change
    create_table :trading_stats_yearlies do |t|
      t.datetime :recorded_date
      t.float :balance_btc
      t.float :balance_btc_sats
      t.float :balance_usd
      t.float :balance_usd_cents
      t.float :win_streak
      t.float :lose_streak
      t.float :draw_streak
      t.float :last_5y_wins
      t.float :last_5y_losses
      t.float :last_5y_draws
      t.float :net_balance_change_btc_absolute
      t.float :net_balance_change_btc_sats_absolute
      t.float :net_balance_change_usd_absolute
      t.float :net_balance_change_usd_cents_absolute
      t.float :net_balance_change_btc_percent
      t.float :net_balance_change_usd_percent
      t.boolean :win
      t.boolean :loss
      t.boolean :draw

      t.datetime :created_at, default: ->{ 'now()' }
      t.datetime :updated_at, default: ->{ 'now()' }
    end
  end
end
