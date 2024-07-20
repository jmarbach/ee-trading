class AddLeverageQuantityToTradeLogs < ActiveRecord::Migration[7.0]
  def change
    add_column :trade_logs, :leverage_quantity, :float, default: 0.0
  end
end
