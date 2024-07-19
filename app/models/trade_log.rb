# == Schema Information
#
# Table name: trade_logs
#
#  id                      :bigint           not null, primary key
#  score_log_id            :bigint
#  created_at              :datetime
#  updated_at              :datetime
#  external_id             :string
#  exchange_name           :string
#  trade_type              :string
#  trade_direction         :string
#  quantity                :float            default(0.0)
#  margin_quantity         :float            default(0.0)
#  open_price              :float            default(0.0)
#  close_price             :float            default(0.0)
#  fixing_price            :float            default(0.0)
#  open_fee                :float            default(0.0)
#  close_fee               :float            default(0.0)
#  creation_timestamp      :bigint
#  market_filled_timestamp :bigint
#  closed_timestamp        :bigint
#  absolute_net_proceeds   :float            default(0.0)
#  percent_net_proceeds    :float            default(0.0)
#  strike                  :float            default(0.0)
#  settlement              :string
#
class TradeLog < ApplicationRecord
end
