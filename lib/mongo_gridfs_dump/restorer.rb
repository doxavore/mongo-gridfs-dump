require 'mongo'

module MongoGridFSDump
  class Restorer
    include Logging

    def initialize(source, dest, prefix = 'fs', options = {})
      @prefix = prefix

      @source_path = File.expand_path(source)

      # Always use safe writes on the dest DB
      dest_conn = Mongo::Connection.from_uri(dest + "?safe=true")
      @dest_db = dest_conn[dest.split('/').last]
      @grid = Mongo::Grid.new(@dest_db, @prefix)

      @source_resolver = PathResolver.new(source)
      @dest_resolver = GridFSResolver.new(@dest_db["#{@prefix}.files"], @prefix)

      @status_every = options[:status_every]

      @include_metadata_hash = if options.key?(:include_metadata_hash)
                                 options[:include_metadata_hash]
                               else
                                 true
                               end
    end

    def delete_last_file!
      return if open_grid_id.nil? || !dest_resolver.file_exists_for_grid_id?(open_grid_id)

      grid.delete(open_grid_id)
      logger.info "Deleted possible partial at GridID##{open_grid_id}"
    end

    def restore
      logger.info "MongoDB GridFS Restore counting files in GridFS and on the file system..."

      pre_restore_file_count = source_resolver.count_files
      pre_restore_grid_count = dest_resolver.count_files
      logger.info "Pre-restore total restored files: #{pre_restore_file_count}"
      logger.info "Pre-restore total GridFS files: #{pre_restore_grid_count}"

      restore_count = 0
      last_restored_id = dest_resolver.find_last_restored_grid_id
      logger.debug "Last restored GridID: #{last_restored_id || '(none)'}"

      source_resolver.each_grid_id(last_restored_id) do |next_id|
        next if dest_resolver.file_exists_for_grid_id?(next_id)

        restore_grid_id(next_id)

        restore_count += 1

        notify_restored(restore_count, pre_restore_file_count - pre_restore_grid_count)
      end

      logger.info "Completed initial file restore. Checking file counts..."

      post_restore_file_count = pre_restore_file_count # should not change here
      post_restore_grid_count = dest_resolver.count_files
      difference = post_restore_file_count - post_restore_grid_count
      if difference > 0
        # It seems we are missing some files, so try working our way backwards
        # until we make up the difference
        logger.info "Attempting to find #{difference} missing GridFS files..."

        # Run again, but start from the beginning this time
        source_resolver.each_grid_id(nil) do |next_id|
          unless dest_resolver.file_exists_for_grid_id?(next_id)
            restore_grid_id(next_id)

            restore_count += 1
            difference -= 1

            notify_restored(restore_count, post_restore_file_count - post_restore_grid_count)
          end

          break if difference <= 0
        end

        if difference > 0
          logger.warn "Unable to resolve discrepancy between GridFS and file system: #{difference}"
        end
      end

      logger.info "Restored #{restore_count} files in this run"
      logger.info "Post-restore total GridFS files: #{post_restore_grid_count}"
      logger.info "Post-restore total restored files: #{post_restore_file_count}"
    end

    private

    attr_reader :dest_resolver,
                :grid,
                :source_resolver,
                :status_every

    def notify_restored(count, total)
      if status_every && (count % status_every) == 0
        pct = (count / total.to_f) * 100
        logger.info "Restored #{count}/#{total} files... (#{pct.round(2)}%)"
      end
    end

    def restore_grid_id(grid_id)
      grid_id = dest_resolver.ensure_bson_id(grid_id)
      file_path = source_resolver.file_path_for_grid_id(grid_id)

      opts = {_id: grid_id,
              # Due to driver internals, this requires a string instead of :content_type
              'contentType' => nil,
              # Perform md5 client/server checks
              w: 1}
      if @include_metadata_hash
        opts[:metadata] ||= {}
        opts[:metadata][:hash] = Digest::SHA512.file(file_path).to_s
      end

      File.open file_path, 'rb' do |file|
        grid.put(file, opts)
      end
    end
  end
end
