class AddDrawToTradingStatsDailies < ActiveRecord::Migration[7.0]
  def change
    add_column :trading_stats_dailies, :draw, :boolean, default: false
  end
end
