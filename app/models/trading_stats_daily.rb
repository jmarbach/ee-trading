# == Schema Information
#
# Table name: trading_stats_dailies
#
#  id                           :bigint           not null, primary key
#  recorded_date                :datetime
#  balance_btc                  :float
#  balance_btc_sats             :float
#  balance_usd                  :float
#  balance_usd_cents            :float
#  win_streak                   :float
#  lose_streak                  :float
#  last_100d_wins               :float
#  last_100d_losses             :float
#  created_at                   :datetime
#  updated_at                   :datetime
#  win                          :boolean          default(FALSE)
#  loss                         :boolean          default(FALSE)
#  draw                         :boolean          default(FALSE)
#  draw_streak                  :float
#  last_100d_draws              :float
#  net_balance_change_btc       :float
#  net_balance_change_btc_sats  :float
#  net_balance_change_usd       :float
#  net_balance_change_usd_cents :float
#
class TradingStatsDaily < ApplicationRecord
end
