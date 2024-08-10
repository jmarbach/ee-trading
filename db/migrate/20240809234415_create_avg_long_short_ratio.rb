class CreateAvgLongShortRatio < ActiveRecord::Migration[7.0]
  def change
    add_column :market_data_logs, :avg_long_short_ratio, :float
  end
end
