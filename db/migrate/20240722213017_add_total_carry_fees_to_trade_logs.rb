class AddTotalCarryFeesToTradeLogs < ActiveRecord::Migration[7.0]
  def change
    add_column :trade_logs, :total_carry_fees, :float
  end
end
