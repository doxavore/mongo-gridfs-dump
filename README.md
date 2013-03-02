Mongo GridFS Dump
=================
Dump a GridFS database to the file system.

This is used to support incremental backups, as long as files are write-once.
Dump your GridFS database to the file system and use more proven backup
mechanisms to secure the data in another location.

Files are dumped in two levels of subdirectories, based on their file ID.
For example, using a destination of `/backups/gridfs`, a fs.files document
with an `_id` of `4f8c973fcc9b365022000005` would be created at
`backups/gridfs/4f/8c/4f8c973fcc9b365022000005`. As such, it's recommended
you dump to file system that can handle a large directory tree. XFS seems
to work well for our purposes.

Warning
-------
This project was created to scratch an itch, and is not of the highest quality.
You are welcomed to submit pull requests for something you feel could be
improved.

Usage
-----
Dumping GridFS files to the file system:

    bundle install
    ./bin/mongo-gridfs-dump -s mongodb://user:pass@127.0.0.1/my_db -d /backups/gridfs -p fs

To get a full list of options:

    ./bin/mongo-gridfs-dump --help

If you perform an integrity check (`-i`) with a dump operation, and the check
fails, the process will exit with status `1`.

Restoring files dumped to the file system back into GridFS:

    bundle install
    ./bin/mongo-gridfs-restore -s /backups/gridfs -d mongodb://user:pass@127.0.0.1/my_db -p fs

To get a full list of options:

    ./bin/mongo-gridfs-restore --help

To-Do
-----
* Restore from file system to GridFS

Contributing
------------
* Find something that could be improved
* Fix that something
* Open a pull request
