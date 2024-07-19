class AddReferenceToScores < ActiveRecord::Migration[7.0]
  def change
    add_reference :score_logs, :market_data_log, foreign_key: true
  end
end
