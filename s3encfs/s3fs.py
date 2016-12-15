## Amazon S3 Client
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from s3encfs.config import Config

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFREG, S_IFLNK
from time import time,mktime
from datetime import datetime as dt

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class S3Client():
    def __init__(self):
        s3config = Config('s3.conf')
        aws_keys = s3config.get_aws_keys()
        conn = S3Connection(aws_keys['key'], aws_keys['secret'])
        bucket_name = 's3-enfs-fuse'
        self.bucket = conn.get_bucket(bucket_name)

    def create_file(self,path):
        newfile = self.bucket.new_key(path)
        newfile.set_contents_from_string('')

    def get_attribute(self, path):
        if self.__is_exists(path) == False:
            raise FuseOSError(ENOENT)
        
        is_dir = self.__is_directory(path)
        mode = (S_IFREG | 777)
        nlink = 1
        if is_dir == True:
            path = '%s/' % path
            mode = (S_IFDIR | 777)
            nlink = 2

        filekey = self.bucket.get_key(path)
        last_modified = time() if filekey == None or filekey.last_modified == None else filekey.last_modified
        if type(last_modified) == str:
            ctime = mtime = atime = mktime(dt.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z').timetuple())
        else:
            ctime = mtime = atime = last_modified

        size = 0 if filekey == None or filekey.last_modified == None else filekey.size
        
        return {'st_ctime': ctime,
                'st_mtime': mtime,
                'st_nlink': nlink,
                'st_atime': atime,
                'st_mode': mode,
                'st_size': size
        }

    def mkdir(self, path):
        newdir = self.bucket.new_key('%s/' % path)
        newdir.set_contents_from_string('')

    def read_file(self, path, size, offset):
        filekey = self.bucket.get_key(path)
        data = filekey.get_contents_as_string()
        return data[offset:offset + size]

    def delete(self, path):
        if self.__is_directory(path):
            filekey = self.bucket.get_key('%s/' % path)
        else:
            filekey = self.bucket.get_key(path)
        filekey.delete()

    def truncate(self, path, length):
        filekey = self.bucket.get_key(path)
        current_data = filekey.get_contents_as_string()
        filekey.set_contents_from_string(current_data[:length])

    def write(self, path, data, offset):
        filekey = self.bucket.get_key(path)
        current_data = filekey.get_contents_as_string()
        current_data = current_data[:offset] + data
        filekey.set_contents_from_string(current_data)
        return len(data)

    def get_filelist(self, path):
        filelist = []
        dirlist = []
        if path != '/':
            prefix='%s/' % path
        else:
            prefix = path
        for key in self.bucket.list(prefix=prefix[1:]):
            if key.name.endswith('/'):
                dirlist.append(key.name)
            else:
                filelist.append(key.name)
        if prefix == '/':
            dirlist1 = [x for x in dirlist if len(x.split('/')) == 2]
            filelist1 = [x for x in filelist if len(x.split('/')) == 1]
        else:
            dirlist1 = [x.split(prefix[1:])[1] for x in dirlist if not x == prefix[1:]]
            filelist1 = [x.split(prefix[1:])[1] for x in filelist if not x == prefix[1:]]

        return filelist1 + dirlist1

    def rename_file(old, new):
        if self.__is_directory(old):
            old_path = '%s/' % old
        else:
            old_path = old
        self.bucket.copy_key(new, self.bucket.name, old_path[1:])
        filekey = self.bucket.get_key(old_path)
        filekey.delete()

    
    def get_usage(self):
        usage_byte = 0
        for key in self.bucket.list():
            usage_byte += key.size
        return usage_byte

    
    def __is_directory(self,path):
        if self.bucket.get_key(path) == None or path == '/':
            return True
        else:
            return False

    def __is_exists(self, path):
        if self.bucket.get_key(path) == None and self.bucket.get_key('%s/' % path) == None:
            return False
        else:
            return True

class S3FS(LoggingMixIn, Operations):
    def __init__(self):
        self.s3client = S3Client()
        self.fd = 0
    
    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def create(self, path, mode):
        self.s3client.create_file(path)
        return 1

    def getattr(self, path, fh=None):
        return self.s3client.get_attribute(path)
    
    def mkdir(self, path, mode):
        self.s3client.mkdir(path)

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        return self.s3client.read_file(path, size, offset)
        
    def readdir(self, path, fh):
        return ['.', '..'] + self.s3client.get_filelist(path)

    def rename(self, old, new):
        self.s3client.rename_file(old, new)

    def rmdir(self, path):
        self.s3client.delete(path)

    def statfs(self, path):
        bs = 512
        virtual_total_byte = 1024 * 1024 * 1024
        usage_byte = self.s3client.get_usage()
        free_byte = virtual_total_byte - usage_byte
        return dict(f_bsize=bs,
                    f_frsize=bs,
                    f_blocks=virtual_total_byte / bs,
                    f_bfree=free_byte / bs,
                    f_bavail=free_byte / bs,
                    f_namemax=256
        )

    def truncate(self, path, length, fh=None):
        self.s3client.truncate(path, length)
        
    def unlink(self, path):
        self.s3client.delete(path)
        
    def write(self, path, data, offset, fh):
        return self.s3client.write(path, data, offset)
