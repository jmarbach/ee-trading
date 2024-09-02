# == Schema Information
#
# Table name: trading_stats_monthlies
#
#  id                                    :bigint           not null, primary key
#  recorded_date                         :datetime
#  balance_btc                           :float
#  balance_btc_sats                      :float
#  balance_usd                           :float
#  balance_usd_cents                     :float
#  win_streak                            :float
#  lose_streak                           :float
#  draw_streak                           :float
#  last_12m_wins                         :float
#  last_12m_losses                       :float
#  last_12m_draws                        :float
#  net_balance_change_btc_absolute       :float
#  net_balance_change_btc_sats_absolute  :float
#  net_balance_change_usd_absolute       :float
#  net_balance_change_usd_cents_absolute :float
#  net_balance_change_btc_percent        :float
#  net_balance_change_usd_percent        :float
#  win                                   :boolean
#  loss                                  :boolean
#  draw                                  :boolean
#  created_at                            :datetime
#  updated_at                            :datetime
#
class TradingStatsMonthly < ApplicationRecord
end