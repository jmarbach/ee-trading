class RemoveFixingPriceFromTradeLog < ActiveRecord::Migration[7.0]
  def change
    remove_column :trade_logs, :fixing_price
  end
end
