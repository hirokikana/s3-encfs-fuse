#!/usr/bin/env python
#-*- coding: utf-8 -*-

from s3encfs.s3fs import S3FS
from sys import argv, exit
import logging
from fuse import FUSE

if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(S3FS(), argv[1], foreground=True)
              
    
                                
        
