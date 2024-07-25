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
#  total_carry_fees        :float
#  absolute_gross_proceeds :float
#  percent_gross_proceeds  :float
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
        entry_margin = lnmarkets_response[:body]['entry_margin'].to_f
        absolute_net_proceeds = lnmarkets_response[:body]['pl'].to_f
        percent_net_proceeds = ((((absolute_net_proceeds + entry_margin) - entry_margin)/entry_margin)*100.0).round(2)

        update_columns(
          open_fee: lnmarkets_response[:body]['opening_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          close_price: lnmarkets_response[:body]['exit_price'],
          absolute_net_proceeds: absolute_net_proceeds,
          percent_net_proceeds: percent_net_proceeds,
          market_filled_timestamp: lnmarkets_response[:body]['market_filled_ts'],
          closed_timestamp: lnmarkets_response[:body]['closed_ts'],
          total_carry_fees: lnmarkets_response[:body]['sum_carry_fees'],
          open: false,
          running: false,
          canceled: false
        )
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
        
        margin = lnmarkets_response[:body]['margin'].to_f
        absolute_net_proceeds = lnmarkets_response[:body]['pl'].to_f
        percent_net_proceeds = ((((absolute_net_proceeds + margin) - margin)/margin)*100.0).round(2)

        # update_columns(
        #   open_fee: lnmarkets_response[:body]['opening_fee'],
        #   close_fee: lnmarkets_response[:body]['closing_fee'],
        #   close_price: lnmarkets_response[:body]['exit_price'],
        #   absolute_net_proceeds: absolute_net_proceeds,
        #   percent_net_proceeds: percent_net_proceeds,
        #   market_filled_timestamp: lnmarkets_response[:body]['market_filled_ts'],
        #   closed_timestamp: lnmarkets_response[:body]['closed_ts'],
        #   total_carry_fees: lnmarkets_response[:body]['sum_carry_fees'],
        #   open: false,
        #   running: false,
        #   canceled: false
        # )

      else
        puts 'Error. Unable to get options trade.'
      end
    end
  end
end
