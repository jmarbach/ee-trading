class AddGrossNetProceedsCols < ActiveRecord::Migration[7.0]
  def change
    add_column :trade_logs, :absolute_gross_proceeds, :float
    add_column :trade_logs, :percent_gross_proceeds, :float
  end
end
