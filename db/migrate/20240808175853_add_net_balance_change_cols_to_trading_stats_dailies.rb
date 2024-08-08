class AddNetBalanceChangeColsToTradingStatsDailies < ActiveRecord::Migration[7.0]
  def change
    add_column :trading_stats_dailies, :net_balance_change_btc_absolute, :float
    add_column :trading_stats_dailies, :net_balance_change_btc_sats_absolute, :float
    add_column :trading_stats_dailies, :net_balance_change_usd_absolute, :float
    add_column :trading_stats_dailies, :net_balance_change_usd_cents_absolute, :float

    add_column :trading_stats_dailies, :net_balance_change_btc_percent, :float
    add_column :trading_stats_dailies, :net_balance_change_usd_percent, :float
  end
end
