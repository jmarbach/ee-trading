class AddUsdColsToTradeLogs < ActiveRecord::Migration[7.0]
  def change
    rename_column :trade_logs, :margin_quantity, :margin_quantity_btc_sats
    rename_column :trade_logs, :quantity, :quantity_usd_cents
    add_column :trade_logs, :margin_quantity_usd_cents, :float
    add_column :trade_logs, :quantity_btc_sats, :float
    add_column :trade_logs, :margin_percent_of_quantity, :float
  end
end
