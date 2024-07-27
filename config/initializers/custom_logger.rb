class CustomLogFormatter < Logger::Formatter
  def call(severity, time, progname, msg)
    "[#{severity}] #{msg}\n"
  end
end

Rails.application.configure do
  config.logger = ActiveSupport::Logger.new(STDOUT)
  config.logger.formatter = CustomLogFormatter.new
end
