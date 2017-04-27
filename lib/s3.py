""" Utilities for accessing data stored in Amazon S3 """

from abc import ABCMeta, abstractmethod
from io import BytesIO
import sys
try:
	import boto3
except ImportError:
	try:
		from boto.s3.connection import S3Connection
		from boto.s3.key import Key
	except ImportError:
		print("ignore")


import botocore


class RigorS3Client(object):
	"""
	Object capable of accessing S3 data

	:param config: configuration data
	:type config: :py:class:`~rigor.config.RigorConfiguration`
	:param str bucket: S3 bucket containing data
	:param str credentials: name of credentials section
	:param str session: the s3 session if one exists.
	"""
	__metaclass__ = ABCMeta

	def __init__(self, config, bucket, credentials=None):
		self._config = config
		self.bucket = bucket
		self._credentials = credentials

	@abstractmethod
	def get(self, key, local_file=None):
		"""
		Retrieves an object from S3

		:param str key: S3 key containing the data
		:param str local_file: path to a local file where data should be written, or :py:const:`None`.
		:return: data, unless a local file is given
		:rtype: file
		"""
		pass

	@abstractmethod
	def put(self, key, data):
		"""
		Uploads data to S3

		:param str key: S3 key where the data will go
		:param data: Either a file-like object, or a path to a destination file
		:type data: :py:class:`str` or :py:class:`file`
		"""
		pass

	@abstractmethod
	def delete(self, key):
		"""sudo 
		Removes data at the given key from S3

		:param str key: S3 key for the object to delete
		"""
		pass

	@abstractmethod
	def list(self, prefix=None):
		"""
		Lists the contents of the current S3 bucket

		:param str prefix: restrict listing to keys with this prefix
		:return: keys in the current bucket
		:rtype: list
		"""
		pass

class BotoS3Client(RigorS3Client):
	"""
	Object capable of accessing S3 data using Boto
	"""

	def __init__(self, config, bucket, credentials=None):
		super(BotoS3Client, self).__init__(config, bucket, credentials)
		connection_args = list()
		if credentials:
			connection_args.append(config.get(credentials, 'aws_access_key_id'))
			connection_args.append(config.get(credentials, 'aws_secret_access_key'))
		if 'boto3' in sys.modules:
			if credentials:
				session = boto3.session.Session(aws_access_key_id=config.get(credentials, 'aws_access_key_id'),
													  aws_secret_access_key=config.get(credentials, 'aws_secret_access_key'))
				self._conn = session.resource('s3')
			else:
				self._conn = boto3.resource('s3')

			self.bucket = self._conn.Bucket(bucket)
		else:
			self._conn = S3Connection(*connection_args)
			self.bucket = self._conn.get_bucket(bucket)

	def get(self, key, local_file=None):
		""" See :py:meth:`RigorS3Client.get` """
		if 'boto3' in sys.modules:
			return self.__boto3_get__(key, local_file)

		fetched_key = self.bucket.get_key(key)
		if fetched_key is None:
			return None
		if local_file is None:
			contents = BytesIO()
			fetched_key.get_file(contents)
			contents.seek(0)
			return contents
		else:
			fetched_key.get_contents_to_filename(local_file)

	def put(self, key, data):
		""" See :py:meth:`RigorS3Client.put` """
		if 'boto3' in sys.modules:
			return self.__boto3_put__(key, data)

		remote_key = Key(self.bucket)
		remote_key.key = key
		if hasattr(data, 'read'):
			remote_key.set_contents_from_file(data)
		else:
			remote_key.set_contents_from_filename(data)

	def delete(self, key):
		""" See :py:meth:`RigorS3Client.delete` """
		if 'boto3' in sys.modules:
			return self.__boto3_delete__(key)

		remote_key = Key(self.bucket)
		remote_key.key = key
		remote_key.delete()

	def list(self, prefix=None):
		""" See :py:meth:`RigorS3Client.list` """
		if 'boto3' in sys.modules:
			return self.__boto3_list__(prefix)

		if prefix is None:
			prefix = ""
		return self.bucket.list(prefix=prefix)

	def __boto3_get__(self, key, local_file=None):
		""" See :py:meth:`RigorS3Client.get` 
			This implementation is speicfic to Boto3
		"""
		s3_obj = BytesIO()
		try:
			if local_file is None:
				self.bucket.download_fileobj(key, s3_obj)
				s3_obj.seek(0)
			else:
				self.bucket.download_file(key, local_file)
				return True
		except botocore.exceptions.ClientError as e:
			return None

		return s3_obj

	def __boto3_put__(self, key, data):
		""" See :py:meth:`RigorS3Client.put` 
			This implementation is specific to Boto3
		"""
		if hasattr(data, 'read'):
			self.bucket.upload_fileobj(data, key)
		else:
			self.bucket.upload_file(data, key)

	def __boto3_delete__(self, key):
		""" See :py:meth:`RigorS3Client.delete` 
			This implementation is specific to Boto3
		"""
		delObject = {'Objects': [{'Key': key}]}
		self.bucket.delete_objects(Delete=delObject)

	def __boto3_list__(self, prefix=None):
		""" See :py:meth:`RigorS3Client.list` 
			This implementation is specific to Boto3
		"""
		if prefix is None:
			return self.bucket.objects.all()
		else:
			return self.bucket.objects.filter(Prefix=prefix)





DefaultS3Client = BotoS3Client
