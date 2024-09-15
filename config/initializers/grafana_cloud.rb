require 'faraday'

class GrafanaCloudInfluxPushAPI
  def initialize(auth_token)
    @endpoint = 'https://influx-prod-10-prod-us-central-0.grafana.net/api/v1/push/influx/write'
    @auth_token = auth_token
    @connection = create_connection
  end

  def push_metrics(metrics)
    puts "Push metrics to Grafana Cloud: #{metrics}"
    begin
      response = @connection.post do |req|
        req.body = metrics
      end
      puts response.status
    rescue => e
      puts 'Error pushing metric to Grafana Cloud:'
      puts e
      response = '400'
    end
    response
  end

  private

  def create_connection
    Faraday.new(url: @endpoint) do |faraday|
      faraday.headers['Authorization'] = "Basic #{@auth_token}"
      faraday.headers['Content-Type'] = 'text/plain'
      faraday.adapter Faraday.default_adapter
    end
  end
end
