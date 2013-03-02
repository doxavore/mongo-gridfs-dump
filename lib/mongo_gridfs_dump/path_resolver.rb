module MongoGridFSDump
  class PathResolver
    attr_reader :root_path

    def initialize(root_path)
      @root_path = File.expand_path root_path
    end

    def count_files
      find_directories(root_path).inject(0) { |total, dir|
        total + find_directories(dir).inject(0) { |subtotal, subdir|
          subtotal + find_files(subdir).count
        }
      }
    end

    def directory_for_grid_id(grid_id)
      raise ArgumentError.new("no grid_id provided") unless grid_id
      grid_id = grid_id.to_s

      File.join(root_path,
                grid_id[0,2],
                grid_id[2,2])
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

      dirs.each do |dir|
        subdirs = find_directories dir
        next if subdirs.empty?

        subdirs.each do |subdir|
          files = find_files subdir
          return File.basename(files.first) unless files.empty?
        end
      end

      nil
    end

    def find_directories(path)
      Dir.glob(File.join(path, "*")).select { |fp|
        File.directory? fp
      }.reverse
    end

    def find_files(path)
      Dir.glob(File.join(path, "*")).select { |fp|
        File.file? fp
      }.reverse
    end
  end
end
