module MongoGridFSDump
  class PathResolver
    attr_reader :root_path

    def initialize(root_path)
      @root_path = File.expand_path root_path
    end

    def count_files
      if can_quick_count?
        `find #{root_path} -type f -print | wc -l`.strip.to_i
      else
        logger.debug "Program 'find' is not in PATH; falling back to slower directory globbing..."
        find_directories(root_path).inject(0) { |total, dir|
          total + find_directories(dir).inject(0) { |subtotal, subdir|
            subtotal + find_files(subdir).count
          }
        }
      end
    end

    def directory_for_grid_id(grid_id)
      raise ArgumentError.new("no grid_id provided") unless grid_id
      grid_id = grid_id.to_s

      File.join(root_path,
                grid_id[0,2],
                grid_id[2,2])
    end

    def each_grid_id(start_grid_id)
      start_grid_id = start_grid_id.to_s if start_grid_id

      find_directories(root_path).each do |dir|
        dir_name = File.basename(dir)
        next if start_grid_id && start_grid_id[0,2] > dir_name

        find_directories(dir).each do |subdir|
          subdir_name = File.basename(dir)
          next if start_grid_id && start_grid_id[2,2] > subdir_name

          find_files(subdir).each do |file|
            grid_id = File.basename(file)
            next if start_grid_id && start_grid_id > grid_id

            yield grid_id 
          end
        end
      end
      nil
    end

    def file_exists_for_grid_id?(grid_id)
      File.exist? file_path_for_grid_id(grid_id)
    end

    def file_path_for_grid_id(grid_id)
      File.join(directory_for_grid_id(grid_id),
                grid_id.to_s)
    end

    def find_last_dumped_grid_id
      dirs = find_directories root_path
      return nil if dirs.empty?

      dirs.reverse.each do |dir|
        subdirs = find_directories dir
        next if subdirs.empty?

        subdirs.reverse.each do |subdir|
          files = find_files subdir
          return File.basename(files.first) unless files.empty?
        end
      end

      nil
    end

    private

    def can_quick_count?
      system("which find > /dev/null 2>&1") &&
        system("which wc > /dev/null 2>&1")
    end

    def find_directories(path)
      Dir.glob(File.join(path, "*")).select { |fp|
        File.directory? fp
      }.sort!
    end

    def find_files(path)
      Dir.glob(File.join(path, "*")).select { |fp|
        File.file? fp
      }.sort!
    end
  end
end
