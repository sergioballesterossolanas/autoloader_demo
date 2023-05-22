# Databricks notebook source
my_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace("@", "_").replace(".", "_")
path = "/dbfs/tmp/test_streaming_read_{}/".format(my_user)
path_checkpoint = "/tmp/test_streaming_read_checkpoint_{}/".format(my_user)