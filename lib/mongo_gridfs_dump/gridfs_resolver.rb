module MongoGridFSDump
  class GridFSResolver

    def initialize(files_collection, prefix = 'fs')
      @files = files_collection
      @prefix = prefix
    end

    def count_files(include_very_recent = false)
      total = files.count
      return total if include_very_recent

      # Throw out the ones we wouldn't dump
      files.find({}, {fields: ['_id']}).sort({'_id' => -1}).each do |doc|
        if doc && doc['_id'] && should_dump_id?(doc['_id'])
          return total
        else
          total -= 1
        end
      end
      total
    end

    def each_grid_id(start_grid_id, ascending = true)
      start_grid_id = ensure_bson_id(start_grid_id)

      while grid_id = next_grid_id(start_grid_id, ascending)
        yield grid_id
        start_grid_id = grid_id
      end
    end

    def ensure_bson_id(id)
      case id
      when BSON::ObjectId
        id
      when String
        BSON::ObjectId.from_string(id)
      when nil
        nil
      else
        raise ArgumentError, "Don't know how to convert #{id.class} to BSON::ObjectId"
      end
    end

    def file_exists_for_grid_id?(grid_id)
      !files.find({_id: ensure_bson_id(grid_id)}).limit(1).first.nil?
    end

    def find_last_restored_grid_id
      doc = files.find({}, {fields: ['_id']}).sort({'_id' => -1}).limit(1).first
      doc ? doc['_id'] : nil
    end

    def server_md5(grid_id)
      grid_id = ensure_bson_id(grid_id)

      md5_command = BSON::OrderedHash.new
      md5_command['filemd5'] = grid_id
      md5_command['root'] = prefix

      server_md5 = files.db.command(md5_command)['md5']
    end

    private

    attr_reader :files, :prefix

    def next_grid_id(prev_id, ascending = true)
      prev_id = ensure_bson_id(prev_id)

      if prev_id
        query_op = ascending ? '$gt' : '$lt'
        query = {'_id' => {query_op => prev_id}}
      else
        # Find the first document, if any
        query = {}
      end

      id_sort = ascending ? 1 : -1
      next_doc = files.find(query, {fields: ['_id']}).sort({'_id' => id_sort}).limit(1).first

      if next_doc
        id = next_doc['_id']

        return id if should_dump_id?(id)
      end

      nil
    end

    def should_dump_id?(grid_id)
      # Only return if not created in the last 60 seconds,
      # so we are certain to not get any partial files
      grid_id && grid_id.generation_time < (Time.now.utc - 60)
    end
  end
end
