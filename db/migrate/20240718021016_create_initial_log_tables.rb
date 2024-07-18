class CreateInitialLogTables < ActiveRecord::Migration[7.0]
  def change
    create_table :market_data_logs do |t|
      t.datetime :recorded_date
      t.float :price_btcusd
      t.float :prior_day_volume
      t.float :two_days_ago_volume
      t.float :three_days_ago_volume
      t.float :four_days_ago_volume
      t.float :five_days_ago_volume
      t.float :six_days_ago_volume
      t.float :seven_days_ago_volume
      t.float :rsi
      t.float :simple_moving_average
      t.float :exponential_moving_average
      t.float :macd_value
      t.float :macd_signal
      t.float :macd_histogram
      t.float :avg_funding_rate
      t.float :aggregate_open_interest
      t.float :avg_last_10_candle_closes
      t.float :avg_last_8_aggregate_open_interests

      t.datetime :created_at, default: ->{ 'now()' }
      t.datetime :updated_at, default: ->{ 'now()' }
    end

    create_table :score_logs do |t|
      t.datetime :recorded_date
      t.float :score

      t.datetime :created_at, default: ->{ 'now()' }
      t.datetime :updated_at, default: ->{ 'now()' }
    end

    create_table :trade_logs do |t|
      t.references :score_log
      t.datetime :recorded_date

      t.datetime :created_at, default: ->{ 'now()' }
      t.datetime :updated_at, default: ->{ 'now()' }
    end
  end
end
