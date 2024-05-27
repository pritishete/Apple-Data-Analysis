# Databricks notebook source
class DataSink:
    """
    Abstract Class
    """

    def __init__(self,df,path,method,param):

        self.df=df
        self.path=path
        self.method=method
        self.param=param

    def load_data_frame(self):
        """
        Abstract method,functions will be defined in sub classes
        """
        raise ValueError("Not implemented") 



class LoadToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartition(DataSink):
    def load_data_frame(self):

        partitionByColumns=self.param.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)

class LoadToDeltaTable(DataSink):
    def load_data_frame(self):
        print("Inside load_delta ()")
        self.df.write.format("delta").mode(self.method).saveAsTable(self.path)        

def get_sink_source(sink_type,df,path,method,params=None):

    if(sink_type=="DBFS"):
        return LoadToDBFS(df,path,method,params)
    elif(sink_type=="DBFS_with_partitions"):
        return LoadToDBFSWithPartition(df,path,method,params)
    elif(sink_type=="delta"):
        return LoadToDeltaTable(df,path,method,params)
    else:
        return(f"Not implemented for sink type:{sink_type}")
    




# COMMAND ----------

