#!/usr/bin/env python
#-*- coding: utf-8 -*-

## Amazon S3 Client
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key

from ConfigParser import SafeConfigParser

config = SafeConfigParser()
config.read('s3.conf')


key = config.get('aws', 'key')
secret_key = config.get('aws', 'secret_key')
conn = S3Connection(key, secret_key)
bucket_name = 's3-enfs-fuse'
bucket = conn.get_bucket(bucket_name)

import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFREG, S_IFLNK
from sys import argv, exit
from time import time,mktime
from datetime import datetime as dt

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

class Memory(LoggingMixIn, Operations):
    def __init__(self):
        self.files = {}
        self.data = defaultdict(str)
        self.fd = 0
        now = time()
        import pdb;pdb.set_trace()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

    def chmod(self, path, mode):
        self.files[path]['st_mode'] &= 0o770000
        self.files[path]['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        self.files[path]['st_uid'] = uid
        self.files[path]['st_gid'] = gid

    def create(self, path, mode):
        newfile = bucket.new_key(path)
        newfile.set_contents_from_string('')
        return 1

    def getattr(self, path, fh=None):
        is_dir = False
        filekey = bucket.get_key(path)

        if filekey == None:
            filekey = bucket.get_key('%s/' % path)
            is_dir = True
            if filekey == None:
                raise FuseOSError(ENOENT)
        if path == '/':
            is_dir = True
        
        now = time()
        ctime = now
        mtime = now
        atime = now
        if filekey:
            try:
                ctime = mktime(dt.strptime(filekey.last_modified, '%a, %d %b %Y %H:%M:%S %Z').timetuple())
                mtime = mktime(dt.strptime(filekey.last_modified, '%a, %d %b %Y %H:%M:%S %Z').timetuple())
                atime = mktime(dt.strptime(filekey.last_modified, '%a, %d %b %Y %H:%M:%S %Z').timetuple())
            except TypeError:
                pass

        if is_dir:
            mode = (S_IFDIR | 777)
            return {'st_ctime': ctime,
                    'st_mtime': mtime,
                    'st_nlink': 2,
                    'st_atime': atime,
                    'st_mode': mode}
        else:
            mode = (S_IFREG | 777)
            return {'st_ctime': ctime,
                    'st_mtime': mtime,
                    'st_nlink': 1,
                    'st_atime': atime,
                    'st_mode': mode,
                    'st_size': filekey.size
            }

    def _getxattr(self, path, name, position=0):
        attrs = self.files[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''

    def _listxattr(self, path):
        attrs = self.files[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        newdir = bucket.new_key('%s/' % path)
        newdir.set_contents_from_string('')

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        filekey = bucket.get_key(path)
        data = filekey.get_contents_as_string()
        return data[offset:offset + size]

    def readdir(self, path, fh):
        filelist = []
        dirlist = []
        if path != '/':
            prefix='%s/' % path
        else:
            prefix = path
        for key in bucket.list(prefix=prefix[1:]):
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
        return ['.', '..'] + filelist1 + dirlist1

    def readlink(self, path):
        return self.data[path]

    def _removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})
        try:
            del attrs[name]
        except KeyError:
            pass

    def rename(self, old, new):
        self.files[new] = self.files.pop(old)

    def rmdir(self, path):
        self.files.pop(path)
        self.files['/']['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        attrs = self.files[path].setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=8192, f_bavail=2048)


    def symlink(self, target, source):
        self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
        self.data[target] = source

    def truncate(self, path, length, fh=None):
        filekey = bucket.get_key(path)
        current_data = filekey.get_contents_as_string()
        filekey.set_contents_from_string(current_data[:length])

    def unlink(self, path):
        self.files.pop(path)

    def utimes(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.files[path]['st_atime'] = atime
        self.files[path]['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        filekey = bucket.get_key(path)
        current_data = filekey.get_contents_as_string()
        current_data = current_data[:offset] + data
        filekey.set_contents_from_string(current_data)
        return len(data)

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
              
    
                                
        
