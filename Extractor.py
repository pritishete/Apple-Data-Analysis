# Databricks notebook source
class extractor:
    """
    Abstract class
    """

    def __init__(self):
        pass

    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading data
        """
        transactionInputDf=get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
            ).get_data_frame()
        
        print("Transaction df:")
        transactionInputDf.orderBy("customer_id","transaction_date").show() 

        

        customerInputDf=get_data_source(
            data_type="delta",
            file_path="default.customer_delta_table"
            ).get_data_frame()

        customerInputDf.show()
        print("customer df:")

        inputDFs={"transactionInputDf":transactionInputDf,
                  "customerInputDf":customerInputDf}

        return inputDFs
    