require 'logger'

module MongoGridFSDump
  def self.logger
    @logger ||= Logger.new(STDOUT).tap { |log|
      log.level = Logger::INFO
    }
  end

  module Logging
    def self.logger
      MongoGridFSDump.logger
    end

    def logger
      MongoGridFSDump.logger
    end
  end
end
