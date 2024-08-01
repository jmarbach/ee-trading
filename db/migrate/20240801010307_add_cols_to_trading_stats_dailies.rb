class AddColsToTradingStatsDailies < ActiveRecord::Migration[7.0]
  def change
    add_column :trading_stats_dailies, :win, :boolean, default: false
    add_column :trading_stats_dailies, :loss, :boolean, default: false
  end
end
