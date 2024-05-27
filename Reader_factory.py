# Databricks notebook source
class DataSource:
    """
    Abstract Class
    """

    def __init__(self,path):
        self.path=path

    def get_data_frame(self):
        """
        Abstract method,functions will be defined in sub classes
        """
        raise ValueError("Not implemented")    


class CSVDataSource(DataSource):
   
    def get_data_frame(self):
        return(
            spark.
            read.
            format("csv").
            option("header",True).
            load(self.path)

        )


class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return(
            spark.
            read.
            format("parquet").
            load(self.path)    
        )

class OrcDataSource(DataSource):
    def get_data_frame(self):
        return(
            spark.
            read.
            format("orc").
            load(self.path)    
        )

class DeltaDataSource(DataSource):
    def get_data_frame(self):
        table_name=self.path

        return(
            spark.
            read.
            table(table_name)    
        )


def get_data_source(data_type,file_path):

    if data_type=='csv':
        print("Inside get_data_source:")
        return CSVDataSource(file_path)
    elif data_type=='parquet':
        return ParquetDataSource(file_path)
    elif data_type=='orc':
        return OrcDataSource(file_path)
    elif data_type=='delta':
        return DeltaDataSource(file_path)
    else:
        raise ValueError(f"Not implemented  for data_type:{data_type}")
    
    


# COMMAND ----------

