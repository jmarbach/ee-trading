# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# This file is the source Rails uses to define your schema when running `bin/rails
# db:schema:load`. When creating a new database, `bin/rails db:schema:load` tends to
# be faster and is potentially less error prone than running all of your
# migrations from scratch. Old migrations may fail to apply correctly if those
# migrations use external dependencies or application code.
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema[7.0].define(version: 2024_08_04_192721) do
  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "market_data_logs", force: :cascade do |t|
    t.datetime "recorded_date"
    t.float "price_btcusd"
    t.float "prior_day_volume"
    t.float "two_days_ago_volume"
    t.float "three_days_ago_volume"
    t.float "four_days_ago_volume"
    t.float "five_days_ago_volume"
    t.float "six_days_ago_volume"
    t.float "seven_days_ago_volume"
    t.float "rsi"
    t.float "simple_moving_average"
    t.float "exponential_moving_average"
    t.float "macd_value"
    t.float "macd_signal"
    t.float "macd_histogram"
    t.float "avg_funding_rate"
    t.float "aggregate_open_interest"
    t.float "avg_last_10_candle_closes"
    t.float "avg_last_8_aggregate_open_interests"
    t.datetime "created_at", default: -> { "now()" }
    t.datetime "updated_at", default: -> { "now()" }
    t.integer "int_data_errors", default: 0
    t.float "implied_volatility_deribit"
    t.float "implied_volatility_t3"
  end

  create_table "score_logs", force: :cascade do |t|
    t.datetime "recorded_date"
    t.float "score"
    t.datetime "created_at", default: -> { "now()" }
    t.datetime "updated_at", default: -> { "now()" }
    t.bigint "market_data_log_id"
    t.index ["market_data_log_id"], name: "index_score_logs_on_market_data_log_id"
  end

  create_table "trade_logs", force: :cascade do |t|
    t.bigint "score_log_id"
    t.datetime "created_at", default: -> { "now()" }
    t.datetime "updated_at", default: -> { "now()" }
    t.string "external_id"
    t.string "exchange_name"
    t.string "trade_type"
    t.string "trade_direction"
    t.float "quantity_usd_cents", default: 0.0
    t.float "margin_quantity_btc_sats", default: 0.0
    t.float "open_price", default: 0.0
    t.float "close_price", default: 0.0
    t.float "open_fee", default: 0.0
    t.float "close_fee", default: 0.0
    t.bigint "creation_timestamp"
    t.bigint "market_filled_timestamp"
    t.bigint "closed_timestamp"
    t.float "net_proceeds_absolute", default: 0.0
    t.float "net_proceeds_percent", default: 0.0
    t.float "strike", default: 0.0
    t.string "settlement"
    t.string "instrument"
    t.string "derivative_type"
    t.float "leverage_quantity", default: 0.0
    t.boolean "open"
    t.boolean "running"
    t.boolean "canceled"
    t.boolean "closed"
    t.bigint "last_update_timestamp"
    t.float "implied_volatility"
    t.float "total_carry_fees"
    t.float "gross_proceeds_absolute", default: 0.0
    t.float "gross_proceeds_percent", default: 0.0
    t.float "margin_quantity_usd_cents"
    t.float "quantity_btc_sats"
    t.float "margin_percent_of_quantity"
    t.index ["score_log_id"], name: "index_trade_logs_on_score_log_id"
  end

  create_table "trading_stats_dailies", force: :cascade do |t|
    t.datetime "recorded_date"
    t.float "balance_btc"
    t.float "balance_btc_sats"
    t.float "balance_usd"
    t.float "balance_usd_cents"
    t.float "win_streak"
    t.float "lose_streak"
    t.float "last_100d_wins"
    t.float "last_100d_losses"
    t.datetime "created_at", default: -> { "now()" }
    t.datetime "updated_at", default: -> { "now()" }
    t.boolean "win", default: false
    t.boolean "loss", default: false
    t.boolean "draw", default: false
    t.float "draw_streak"
    t.float "last_100d_draws"
  end

  add_foreign_key "score_logs", "market_data_logs"
end
