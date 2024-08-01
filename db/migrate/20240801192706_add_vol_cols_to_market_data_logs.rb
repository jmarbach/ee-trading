class AddVolColsToMarketDataLogs < ActiveRecord::Migration[7.0]
  def change
    rename_column :market_data_logs, :implied_volatility, :implied_volatility_deribit
    add_column :market_data_logs, :implied_volatility_t3, :float
  end
end
