require 'logger'

module MongoGridFSDump
  def self.logger
    @logger ||= Logger.new(STDOUT).tap { |log|
      log.level = Logger::INFO
    }
  end

  module Logging
    STORAGE_UNITS = %w(bytes KiB MiB GiB TiB)

    def self.logger
      MongoGridFSDump.logger
    end

    def logger
      MongoGridFSDump.logger
    end

    # Lovingly borrowed from Rails ActionPack
    def number_to_human_size(number)
      base = 1024

      if number.to_i < base
        "#{number} bytes"
      else
        number   = number.to_f
        max_exp  = STORAGE_UNITS.size - 1
        exponent = (Math.log(number) / Math.log(base)).to_i # Convert to base
        exponent = max_exp if exponent > max_exp # we need this to avoid overflow for the highest unit
        number  /= base ** exponent

        unit_key = STORAGE_UNITS[exponent]

        "#{number.round(2)} #{unit_key.to_s}"
      end

    end
  end
end
