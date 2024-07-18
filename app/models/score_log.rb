# == Schema Information
#
# Table name: score_logs
#
#  id            :bigint           not null, primary key
#  recorded_date :datetime
#  score         :float
#  created_at    :datetime
#  updated_at    :datetime
#
class ScoreLog < ApplicationRecord
end
