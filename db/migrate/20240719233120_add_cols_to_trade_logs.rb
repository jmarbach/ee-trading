class AddColsToTradeLogs < ActiveRecord::Migration[7.0]
  def change
    remove_column :trade_logs, :recorded_date, :datetime
    add_column :trade_logs, :external_id, :string
    add_column :trade_logs, :exchange_name, :string
    add_column :trade_logs, :trade_type, :string
    add_column :trade_logs, :trade_direction, :string
    add_column :trade_logs, :quantity, :float, default: 0.0
    add_column :trade_logs, :margin_quantity, :float, default: 0.0
    add_column :trade_logs, :open_price, :float, default: 0.0
    add_column :trade_logs, :close_price, :float, default: 0.0
    add_column :trade_logs, :fixing_price, :float, default: 0.0
    add_column :trade_logs, :open_fee, :float, default: 0.0
    add_column :trade_logs, :close_fee, :float, default: 0.0
    add_column :trade_logs, :creation_timestamp, :bigint
    add_column :trade_logs, :market_filled_timestamp, :bigint
    add_column :trade_logs, :closed_timestamp, :bigint
    add_column :trade_logs, :absolute_net_proceeds, :float, default: 0.0
    add_column :trade_logs, :percent_net_proceeds, :float, default: 0.0
    add_column :trade_logs, :strike, :float, default: 0.0
    add_column :trade_logs, :settlement, :string
  end
end
