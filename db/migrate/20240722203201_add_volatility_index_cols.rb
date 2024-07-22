class AddVolatilityIndexCols < ActiveRecord::Migration[7.0]
  def change
    add_column :market_data_logs, :implied_volatility, :float
    add_column :trade_logs, :implied_volatility, :float
  end
end
