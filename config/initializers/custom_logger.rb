class CustomLogFormatter < Logger::Formatter
  def call(severity, time, progname, msg)
    "[#{severity}] #{msg}\n"
  end
end
