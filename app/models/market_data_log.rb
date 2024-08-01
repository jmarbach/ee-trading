# == Schema Information
#
# Table name: market_data_logs
#
#  id                                  :bigint           not null, primary key
#  recorded_date                       :datetime
#  price_btcusd                        :float
#  prior_day_volume                    :float
#  two_days_ago_volume                 :float
#  three_days_ago_volume               :float
#  four_days_ago_volume                :float
#  five_days_ago_volume                :float
#  six_days_ago_volume                 :float
#  seven_days_ago_volume               :float
#  rsi                                 :float
#  simple_moving_average               :float
#  exponential_moving_average          :float
#  macd_value                          :float
#  macd_signal                         :float
#  macd_histogram                      :float
#  avg_funding_rate                    :float
#  aggregate_open_interest             :float
#  avg_last_10_candle_closes           :float
#  avg_last_8_aggregate_open_interests :float
#  created_at                          :datetime
#  updated_at                          :datetime
#  int_data_errors                     :integer          default(0)
#  implied_volatility_deribit          :float
#  implied_volatility_t3               :float
#
class MarketDataLog < ApplicationRecord
end
