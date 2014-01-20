#!/usr/bin/python _tt

from pymongo import MongoClient
import socket
import os

statsd_namespace = 'a.statsd.namespace'
statsd_host = "A_STATSD_HOST"
statsd_port = 8125
statsd_connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

mongodb_directory = '/the/mongo/directory/'
mongodb_database = 'database-name'
mongo_client = MongoClient()
db = mongo_client[mongodb_database]
mongo_stats = db.command("dbstats")

index_size = (mongo_stats["indexSize"] / 1024) / 1024
file_size = (mongo_stats["fileSize"]  / 1024) / 1024
ns_size = mongo_stats["nsSizeMB"]
objects_count = (mongo_stats["objects"] / 1024) / 1024
avg_object_size = (mongo_stats["avgObjSize"] / 1024) / 1024
total_file_size = file_size + ns_size

directory_size = (sum(os.path.getsize(os.path.join(dirpath,filename)) for dirpath, dirnames, filenames in os.walk(mongodb_directory) for filename in filenames) / 1024) / 1024

statsd_connection.sendto('{ns}.indexSize:{counter}|g'.format(ns=statsd_namespace,counter=index_size), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.fileSize:{counter}|g'.format(ns=statsd_namespace,counter=file_size), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.namespaceSize:{counter}|g'.format(ns=statsd_namespace,counter=ns_size), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.objectsSize:{counter}|g'.format(ns=statsd_namespace,counter=objects_count), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.avgObjectSize:{counter}|g'.format(ns=statsd_namespace,counter=avg_object_size), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.totalFileSize:{counter}|g'.format(ns=statsd_namespace,counter=total_file_size), (statsd_host, statsd_port))

statsd_connection.sendto('{ns}.directorySize:{counter}|g'.format(ns=statsd_namespace,counter=directory_size), (statsd_host, statsd_port))
