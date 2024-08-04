class AddDrawStreaksToTradingStatsDailies < ActiveRecord::Migration[7.0]
  def change
    add_column :trading_stats_dailies, :draw_streak, :float
    add_column :trading_stats_dailies, :last_100d_draws, :float
  end
end
