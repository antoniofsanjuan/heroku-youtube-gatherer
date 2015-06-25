#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

import os
import sys
import boto.s3
from boto.s3.key import Key

class S3manager():

    _bucket = None
    AWS_ACCESS_KEY_ID = 'AKIAJC6RUE5GKMNXVXEA'
    AWS_SECRET_ACCESS_KEY = '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'

    _bucket = None
    _bucket_name = None
    _conn = None

    def __init__(self):

        #self._bucket_name = self.AWS_ACCESS_KEY_ID.lower() + '-' + 'heroku-ytg'
        self._bucket_name = 'heroku-ytg-v2'
        self._conn = boto.connect_s3()

        # self._conn = boto.connect_s3() This way get the ID and pass from default user folder "~/.aws/credentials" file

        allBuckets = self._conn.get_all_buckets()
        for bucket in allBuckets:
           print(str(bucket.name))

        try:
            if (self._conn is not None):
                self._bucket = self._conn.get_bucket(self._bucket_name)
            else:
                print "Connection is None"
        except boto.exception.S3ResponseError as e:
            if (self._bucket is not None):
                print "Bucket is NOT None."
            else:
                print "Bucket IS None.  Except: %s" % e
                exit(1)


    def uploadFile(self, path_filename):

        #print 'Uploading %s to Amazon S3 bucket %s' % \
        #       (path_filename, self._bucket_name)

        try:
            path, filename = os.path.split(path_filename)

            k = self._bucket.new_key(filename)
            k.set_contents_from_filename(path_filename, cb=self.percent_cb, num_cb=10)

        except Exception as e:
            print "Exception uploading file '%s' to S3" % path_filename
            return 1

        return True

    def percent_cb(self, complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()



def main(argv):
    print ""
    s3_service = S3manager()

    # Uncomment to test
    #s3_service.uploadFile('DATA/xxx.csv')


if __name__ == '__main__':
    main(sys.argv)
