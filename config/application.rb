require_relative "boot"

require "rails/all"

require_relative '../lib/custom_log_formatter'

# Require the gems listed in Gemfile, including any gems
# you've limited to :test, :development, or :production.
Bundler.require(*Rails.groups)

module EeTrading
  class Application < Rails::Application
    # Initialize configuration defaults for originally generated Rails version.
    config.load_defaults 7.0

    # Custom logger
    config.logger = ActiveSupport::Logger.new(STDOUT)
    config.logger.formatter = ::CustomLogFormatter.new

    # Configuration for the application, engines, and railties goes here.
    #
    # These settings can be overridden in specific environments using the files
    # in config/environments, which are processed later.
    #
    # config.time_zone = "Central Time (US & Canada)"
    # config.eager_load_paths << Rails.root.join("extras")

    # Auto-load lib files
    config.autoload_paths << Rails.root.join('lib')
  end
end
