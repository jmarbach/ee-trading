class AddStrategyNameToMarketDataLogsAndTradeLogs < ActiveRecord::Migration[7.0]
  def change
    add_column :market_data_logs, :strategy, :string
    add_column :trade_logs, :strategy, :string
  end
end
