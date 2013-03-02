require 'mongo'

module MongoGridFSDump
  class Restorer
    include Logging

    def initialize(source, dest, prefix = 'fs', options = {})
      @source_path = File.expand_path(source)

      # Always use safe writes on the dest DB
      dest_conn = Mongo::Connection.from_uri(dest + "?safe=true")
      @dest_db = dest_conn[dest.split('/').last]

      @prefix = prefix
    end

    def restore
      puts "Restoring is not yet implemented..."
    end

    private

    attr_reader :dest_db,
                :prefix,
                :source_path
  end
end
