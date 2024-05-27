# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self,TransformedDF):
        self.TransformedDF=TransformedDF

    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):
    def sink(self):
        get_sink_source(sink_type="DBFS",df=self.TransformedDF,path="/dbfs/FileStore/tables/AirpodsafterIphone",method="overwrite").load_data_frame()

    

# COMMAND ----------

class OnlyAirpodsAndIphoneLoader(AbstractLoader):
    def sink(self):
        params={"partitionByColumns":["location"]}
        get_sink_source(sink_type="DBFS_with_partitions",df=self.TransformedDF,path="/dbfs/FileStore/tables/OnlyAirpodAndIphone",method="overwrite",params=params).load_data_frame()

        print("Writing delta table started ")
        get_sink_source(sink_type="delta",df=self.TransformedDF,path="default.onlyAirpodsAfterIphone",method="overwrite").load_data_frame()

        print("Writing delta table completed ")

# COMMAND ----------

class ListProductsAfterInitialLoader(AbstractLoader):
    def sink(self):
        get_sink_source(sink_type="DBFS",df=self.TransformedDF,path="/dbfs/FileStore/tables/ListProductsAfterInitialPurchase",method="overwrite").load_data_frame()

# COMMAND ----------

class AverageDelayTimeLoader(AbstractLoader):
    def sink(self):
        get_sink_source(sink_type="DBFS",df=self.TransformedDF,path="/dbfs/FileStore/tables/AverageDelayAirpodsIphone",method="overwrite").load_data_frame()