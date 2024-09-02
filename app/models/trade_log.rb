# == Schema Information
#
# Table name: trade_logs
#
#  id                         :bigint           not null, primary key
#  score_log_id               :bigint
#  created_at                 :datetime
#  updated_at                 :datetime
#  external_id                :string
#  exchange_name              :string
#  trade_type                 :string
#  trade_direction            :string
#  quantity_usd_cents         :float            default(0.0)
#  margin_quantity_btc_sats   :float            default(0.0)
#  open_price                 :float            default(0.0)
#  close_price                :float            default(0.0)
#  open_fee                   :float            default(0.0)
#  close_fee                  :float            default(0.0)
#  creation_timestamp         :bigint
#  market_filled_timestamp    :bigint
#  closed_timestamp           :bigint
#  net_proceeds_absolute      :float            default(0.0)
#  net_proceeds_percent       :float            default(0.0)
#  strike                     :float            default(0.0)
#  settlement                 :string
#  instrument                 :string
#  derivative_type            :string
#  leverage_quantity          :float            default(0.0)
#  open                       :boolean
#  running                    :boolean
#  canceled                   :boolean
#  closed                     :boolean
#  last_update_timestamp      :bigint
#  implied_volatility         :float
#  total_carry_fees           :float
#  gross_proceeds_absolute    :float            default(0.0)
#  gross_proceeds_percent     :float            default(0.0)
#  margin_quantity_usd_cents  :float
#  quantity_btc_sats          :float
#  margin_percent_of_quantity :float
#  strategy                   :string
#
class TradeLog < ApplicationRecord

  def get_final_trade_stats
    Rails.logger.info(
      {
        message: "TradeLog - get_final_trade_stats - #{self.id}",
        script: "TradeLog:get_final_trade_stats"
      }.to_json
    )
  	#
    # Get final trade stats from LnMarkets
    #
    lnmarkets_client = LnMarketsAPI.new
    if self.derivative_type == 'futures'
      lnmarkets_response = lnmarkets_client.get_futures_trade(self.external_id)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "Parse trade response from LnMarkets",
            body: "#{lnmarkets_response[:body]}",
            script: "TradeLog:get_final_trade_stats"
          }.to_json
        )
        #
        # Update TradeLog
        #
        entry_margin = lnmarkets_response[:body]['entry_margin'].to_f
        gross_proceeds_absolute = lnmarkets_response[:body]['pl'].to_f
        gross_proceeds_percent = ((((gross_proceeds_absolute + entry_margin) - entry_margin)/entry_margin)*100.0).round(2)

        sum_fees = (lnmarkets_response[:body]['opening_fee'] + lnmarkets_response[:body]['closing_fee'] + lnmarkets_response[:body]['sum_carry_fees'])
        net_proceeds_absolute = (gross_proceeds_absolute - sum_fees)
        net_proceeds_percent = ((((net_proceeds_absolute + entry_margin) - entry_margin)/entry_margin)*100.0).round(2)

        update_columns(
          open_fee: lnmarkets_response[:body]['opening_fee'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          close_price: lnmarkets_response[:body]['exit_price'],
          gross_proceeds_absolute: gross_proceeds_absolute,
          gross_proceeds_percent: gross_proceeds_percent,
          net_proceeds_absolute: net_proceeds_absolute,
          net_proceeds_percent: net_proceeds_percent,
          market_filled_timestamp: lnmarkets_response[:body]['market_filled_ts'],
          closed_timestamp: lnmarkets_response[:body]['closed_ts'],
          total_carry_fees: lnmarkets_response[:body]['sum_carry_fees'],
          open: false,
          running: false,
          canceled: false,
          last_update_timestamp: lnmarkets_response[:body]['last_update_ts']
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to get futures trade.",
            script: "TradeLog:get_final_trade_stats"
          }.to_json
        )
      end
    elsif self.derivative_type == 'options'
      lnmarkets_response = lnmarkets_client.get_options_trade(self.external_id)
      if lnmarkets_response[:status] == 'success'
        Rails.logger.info(
          {
            message: "Parse trade response from LnMarkets",
            body: "#{lnmarkets_response[:body]}",
            script: "TradeLog:get_final_trade_stats"
          }.to_json
        )
        #
        # Update TradeLog
        #
        margin = lnmarkets_response[:body]['margin'].to_f
        gross_proceeds_absolute = lnmarkets_response[:body]['pl'].to_f
        gross_proceeds_percent = ((((gross_proceeds_absolute + margin) - margin)/margin)*100.0).round(2)

        sum_fees = (lnmarkets_response[:body]['opening_fee'].to_f + lnmarkets_response[:body]['closing_fee'].to_f)
        net_proceeds_absolute = (gross_proceeds_absolute - (sum_fees))
        net_proceeds_percent = ((((net_proceeds_absolute + margin) - margin)/margin)*100.0).round(2)

        update_columns(
          close_price: lnmarkets_response[:body]['fixing_price'],
          closed_timestamp: lnmarkets_response[:body]['closed_ts'],
          close_fee: lnmarkets_response[:body]['closing_fee'],
          gross_proceeds_absolute: gross_proceeds_absolute,
          gross_proceeds_percent: gross_proceeds_percent,
          net_proceeds_absolute: net_proceeds_absolute,
          net_proceeds_percent: net_proceeds_percent,
          open: false,
          running: false,
          canceled: false
        )
      else
        Rails.logger.fatal(
          {
            message: "Error. Unable to get options trade.",
            script: "TradeLog:get_final_trade_stats"
          }.to_json
        )
      end
    end
  end

  def self.find_or_create_from_external_id(trade_data, derivative_type)
    trade_log = find_by_external_id(trade_data['id'])
    return trade_log if trade_log

    logger.warn("Unable to fetch internal TradeLog record for #{trade_data['id']}. Creating new record with strategy 'unknown'.")

    # Validate input for derivative_type input
    if !["futures", "options"].include?(derivative_type)
      logger.error("Invalid derivative_type param: #{derivative_type}")
    end

    create_from_trade_data(trade_data, derivative_type, 'unknown')
  end  

  private

  def self.create_from_trade_data(trade_data, derivative_type, strategy)
    # derivative_type = derivative_type
    # trade_type = trade_data['side'] == 'b' ? 'buy' : 'sell'
    # trade_direction = trade_data['side'] == 'b' ? 'long' : 'short'

    # common_attributes = {
    #   external_id: trade_data['id'],
    #   strategy: strategy,
    #   derivative_type: derivative_type,
    #   exchange_name: 'lnmarkets',
    #   trade_type: trade_type,
    #   trade_direction: trade_direction,
    #   quantity_usd_cents: (trade_data['quantity'] * 100.0).to_i,
    #   quantity_btc_sats: calculate_quantity_btc_sats(trade_data),
    #   open_fee: trade_data['opening_fee'],
    #   close_fee: trade_data['closing_fee'],
    #   margin_quantity_btc_sats: trade_data['margin'],
    #   margin_quantity_usd_cents: calculate_margin_usd_cents(trade_data),
    #   open_price: trade_data['entry_price'] || trade_data['forward'],
    #   creation_timestamp: trade_data['creation_ts'],
    #   running: trade_data['running'],
    #   closed: trade_data['closed'],
    #   margin_percent_of_quantity: calculate_margin_percent(trade_data)
    # }

    # type_specific_attributes = if derivative_type == 'futures'
    #   {
    #     leverage: trade_data['leverage'],
    #     liquidation_price: trade_data['liquidation'],
    #     stoploss: trade_data['stoploss'],
    #     takeprofit: trade_data['takeprofit'],
    #     sum_carry_fees: trade_data['sum_carry_fees']
    #   }
    # else  # options
    #   {
    #     implied_volatility: trade_data['volatility'],
    #     settlement: trade_data['settlement'],
    #     expiry_timestamp: trade_data['expiry_ts'],
    #     strike_price: trade_data['strike'],
    #     option_type: trade_data['type'] == 'c' ? 'call' : 'put',
    #     instrument: trade_data['instrument_name'] || "BTC-USD-#{derivative_type.upcase}"
    #   }
    # end

    # create(common_attributes.merge(type_specific_attributes))
  end

  def self.get_current_price_btcusd()
    lnmarkets_client = LnMarketsAPI.new
    response = lnmarkets_client.get_price_btcusd_ticker
    if response[:status] == 'success'
      response[:body]['index']
    else
      logger.error("Failed to fetch current BTC price.")
    end
  end
end
