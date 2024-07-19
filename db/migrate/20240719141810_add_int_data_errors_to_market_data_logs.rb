class AddIntDataErrorsToMarketDataLogs < ActiveRecord::Migration[7.0]
  def change
    add_column :market_data_logs, :int_data_errors, :integer, default: 0
  end
end
