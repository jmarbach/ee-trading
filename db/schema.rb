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

ActiveRecord::Schema[7.0].define(version: 2024_07_19_003204) do
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
    t.datetime "recorded_date"
    t.datetime "created_at", default: -> { "now()" }
    t.datetime "updated_at", default: -> { "now()" }
    t.index ["score_log_id"], name: "index_trade_logs_on_score_log_id"
  end

  add_foreign_key "score_logs", "market_data_logs"
end
