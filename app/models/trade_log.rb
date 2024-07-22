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
#  open_fee                :float            default(0.0)
#  close_fee               :float            default(0.0)
#  creation_timestamp      :bigint
#  market_filled_timestamp :bigint
#  closed_timestamp        :bigint
#  absolute_net_proceeds   :float            default(0.0)
#  percent_net_proceeds    :float            default(0.0)
#  strike                  :float            default(0.0)
#  settlement              :string
#  instrument              :string
#  derivative_type         :string
#  leverage_quantity       :float            default(0.0)
#  open                    :boolean
#  running                 :boolean
#  canceled                :boolean
#  closed                  :boolean
#  last_update_timestamp   :bigint
#  implied_volatility      :float
#
class TradeLog < ApplicationRecord

  after_update :get_final_trade_stats, if: Proc.new { |i| 
    i.saved_change_to_attribute?(:closed, to: true) 
  }

  def get_final_trade_stats
  	puts 'TradeLog - get_final_trade_stats'
  	#
  	# Get final trade stats from LnMarkets
  	#
  	lnmarkets_client = LnmarketsAPI.new
  	if self.derivative_type == 'futures'
  	  lnmarkets_response = lnmarkets_client.get_futures_trade(self.external_id)
      if lnmarkets_response[:status] == 'success'
      	puts 'Parse trade:'
        puts lnmarkets_response[:body]
        #
        # Update TradeLog
        #
      else
        puts 'Error. Unable to get futures trade.'
      end
  	elsif self.derivative_type == 'options'
  	  lnmarkets_response = lnmarkets_client.get_options_trade(self.external_id)
      if lnmarkets_response[:status] == 'success'
      	puts 'Parse trade:'
        puts lnmarkets_response[:body]
        #
        # Update TradeLog
        #
      else
        puts 'Error. Unable to get options trade.'
      end
  	end
  end
end
