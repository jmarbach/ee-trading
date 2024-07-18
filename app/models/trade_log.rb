# == Schema Information
#
# Table name: trade_logs
#
#  id            :bigint           not null, primary key
#  score_log_id  :bigint
#  recorded_date :datetime
#  created_at    :datetime
#  updated_at    :datetime
#
class TradeLog < ApplicationRecord
end
