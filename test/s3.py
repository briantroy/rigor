try:
	import boto3
except ImportError:
	import boto
	from boto.s3.key import Key

from moto import mock_s3
import constants
import os
import sys

mMock = None
kKeys = ('s3-key-1', 's3-key-2', 's3-key-3')

def setup_module(module):
	global mMock
	mMock = mock_s3()
	mMock.start()

	if 'boto3' in sys.modules:
		conn = boto3.resource('s3')
		bucket = conn.create_bucket(Bucket=constants.kExampleBucket)
		for index, key_name in enumerate(kKeys):
			bucket.Object(key_name).put(Body=str(index))
	else:
		conn = boto.connect_s3()
		bucket = conn.create_bucket(constants.kExampleBucket)

		for index, key_name in enumerate(kKeys):
			key = Key(bucket)
			key.key = key_name
			key.set_contents_from_string(str(index))


def teardown_module(module):
	global mMock
	mMock.stop()
	mMock = None
	try:
		os.unlink(constants.kExampleDownloadedFile)
	except OSError:
		pass
