require 'digest/md5'
require 'fileutils'
require 'mongo'

module MongoGridFSDump
  class Dumper
    include Logging

    def initialize(source, dest, prefix = 'fs', options = {})
      @prefix = prefix

      source_conn = Mongo::Connection.from_uri(source)
      @source_db = source_conn[source.split('/').last]
      @grid = Mongo::Grid.new(@source_db, @prefix)

      @dest_resolver = PathResolver.new(dest)
      @source_resolver = GridFSResolver.new(@source_db["#{@prefix}.files"], @prefix)

      @status_every = options[:status_every]
    end

    def delete_last_file!
      return if open_file_path.nil? || File.exist?(open_file_path)

      File.delete(open_file_path)
      logger.info "Deleted possible partial at #{open_file_path}"
    end

    def dump
      logger.info "MongoDB GridFS Dump counting files in GridFS and on the file system..."

      pre_dump_file_count = dest_resolver.count_files
      pre_dump_grid_count = source_resolver.count_files
      logger.info "Pre-dump total GridFS files: #{pre_dump_grid_count}"
      logger.info "Pre-dump total dumped files: #{pre_dump_file_count}"

      dump_count = 0
      last_dumped_id = dest_resolver.find_last_dumped_grid_id
      logger.debug "Last dumped GridID: #{last_dumped_id}"

      while next_id = source_resolver.next_grid_id(last_dumped_id)
        dump_grid_id(next_id)

        dump_count += 1
        last_dumped_id = next_id

        notify_dumped(dump_count, pre_dump_grid_count - pre_dump_file_count)
      end

      logger.info "Completed initial file dump. Checking file counts..."

      post_dump_file_count = dest_resolver.count_files
      post_dump_grid_count = source_resolver.count_files
      difference = post_dump_grid_count - post_dump_file_count
      if difference > 0
        # It seems we are missing some files, so try working our way backwards
        # until we make up the difference
        logger.info "Attempting to find #{difference} missing file system files..."

        last_dumped_id = nil
        while next_id = source_resolver.next_grid_id(last_dumped_id, false)
          unless dest_resolver.file_exists_for_grid_id?(next_id)
            dump_grid_id(next_id)
            dump_count += 1
            difference -= 1

            notify_dumped(dump_count, pre_dump_grid_count - pre_dump_file_count)
          end

          last_dumped_id = next_id
          break if difference <= 0
        end

        if difference > 0
          logger.warn "Unable to resolve discrepancy between GridFS and file system"
        end
      end

      logger.info "Dumped #{dump_count} files in this run"
      logger.info "Post-dump total GridFS files: #{source_resolver.count_files}"
      logger.info "Post-dump total dumped files: #{dest_resolver.count_files}"
    end

    private

    attr_reader :dest_resolver,
                :grid,
                :open_file_path,
                :source_db,
                :source_resolver,
                :status_every

    def dump_grid_id(grid_id)
      dump_path = dest_resolver.file_path_for_grid_id(grid_id)
      dir_path = File.dirname(dump_path)
      FileUtils.mkdir_p(dir_path) unless Dir.exist?(dir_path)

      grid_file = grid.get(grid_id)
      local_md5 = Digest::MD5.new

      begin
        @open_file_path = dump_path
        File.open dump_path, 'wb' do |dump_io|
          grid_file.each do |chunk|
            dump_io.write chunk
            local_md5.update chunk
          end
        end

        server_md5 = source_resolver.server_md5(grid_id)
        logger.debug "GridID##{grid_id} server_md5=#{server_md5}, local_md5=#{local_md5.hexdigest}"

        if local_md5.hexdigest != server_md5
          raise Mongo::GridMD5Failure
        end
      rescue
        # Don't allow a partial file to stick around
        File.delete(dump_path) rescue nil
        raise
      end

      @open_file_path = nil
    end

    def notify_dumped(count, total)
      if status_every && (count % status_every) == 0
        pct = (count / total.to_f) * 100
        logger.info "Dumped #{count}/#{total} files... (#{pct.round(2)}%)"
      end
    end
  end
end
