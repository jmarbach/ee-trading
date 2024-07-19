class AddInstrumentToTradeLog < ActiveRecord::Migration[7.0]
  def change
    add_column :trade_logs, :instrument, :string
    add_column :trade_logs, :derivative_type, :string
  end
end
