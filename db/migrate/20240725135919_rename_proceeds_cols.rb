class RenameProceedsCols < ActiveRecord::Migration[7.0]
  def change
    rename_column :trade_logs, :absolute_net_proceeds, :net_proceeds_absolute
    rename_column :trade_logs, :percent_net_proceeds, :net_proceeds_percent
    rename_column :trade_logs, :absolute_gross_proceeds, :gross_proceeds_absolute
    rename_column :trade_logs, :percent_gross_proceeds, :gross_proceeds_percent
    change_column_default :trade_logs, :gross_proceeds_absolute, from: nil, to: 0.0
    change_column_default :trade_logs, :gross_proceeds_percent, from: nil, to: 0.0
  end
end