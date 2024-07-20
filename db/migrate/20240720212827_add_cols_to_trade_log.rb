class AddColsToTradeLog < ActiveRecord::Migration[7.0]
  def change
    add_column :trade_logs, :open, :boolean
    add_column :trade_logs, :running, :boolean
    add_column :trade_logs, :canceled, :boolean
    add_column :trade_logs, :closed, :boolean
    add_column :trade_logs, :last_update_timestamp, :bigint
  end
end
