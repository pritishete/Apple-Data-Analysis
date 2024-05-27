# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead,col,broadcast,collect_set,size,array_contains,row_number,when,datediff

class Transformer:

    def __init__(self):
        pass

    def transform(self,inputDFs):
        pass

class  AirpodsAfterIphoneTransformer(Transformer):

    def transform(self,inputDFs):
        """
            Customers who bought Airpods after buying the iphone
        """

        transactionInputDf=inputDFs.get("transactionInputDf")

        print("transactionInputDf in transform")

        transactionInputDf.show()
        print("data fetching in transform")
        windowspec=Window.partitionBy("customer_id").orderBy("transaction_date")
        transformDf=transactionInputDf.withColumn("next_product_name",lead("product_name").over(windowspec))

        print("Customer and their next product ")
        transformDf.orderBy("customer_id","transaction_date").show()

        filterDF=transformDf.filter((col("product_name")=="iPhone") & (col("next_product_name")=="AirPods"))

        print("Customer who bought airpods after iphone")
        filterDF.orderBy("customer_id","transaction_date").show()

        customerInputDf=inputDFs.get("customerInputDf")

        joinDF=customerInputDf.join(broadcast(filterDF),"customer_id")
        return joinDF
        



    

# COMMAND ----------

class OnlyAirpodsAndIphone(Transformer):

    def transform(self,inputDFs):
        """
        Customer who bought only Airpods and Iphone
        """

        transactionInputDf=inputDFs.get("transactionInputDf")

        print("transactionInputDf in OnlyAirpodsAndIphone class transform function")

        transactionInputDf.show()
        print("data fetching in OnlyAirpodsAndIphone class transform")

        groupedDF=transactionInputDf.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        print("Grouped DF:")
        groupedDF.show(truncate=False)

        filterDF= groupedDF.filter(array_contains(col("products"),"AirPods") &
                                   array_contains(col("products"),"iPhone") &
                                   (size(col("products"))==2))
        
        print("Filtered grouped DF")
        filterDF.show()
        customerInputDf=inputDFs.get("customerInputDf")

        joinDF=customerInputDf.join(broadcast(filterDF),"customer_id")
        return joinDF


# COMMAND ----------

class ListAllProducts(Transformer):

    def transform(self,inputDFs):
        """
        List out all the products after initial purchase
        """

        transactionInputDf=inputDFs.get("transactionInputDf")

        print("transactionInputDf in ListAllProducts class transform function")

        transactionInputDf.show()
        
        window=Window.partitionBy("customer_id").orderBy("transaction_date")

        print("data fetching in ListAllProducts class transform")
        transformdf=transactionInputDf.withColumn("row_num",row_number().over(window))
        print("transformed df in listallproducts:")
        transformdf.show(truncate=False)

        filter_df=transformdf.filter(col("row_num")!=1)

        print("filterdf in listallproducts ")

        filter_df.show(truncate=False)
        
        list_filter_df=filter_df.groupBy("customer_id").agg(collect_set("product_name").alias("product_list"))

        print("list_filter_df :")
        list_filter_df.show(truncate=False)

        customerInputDf=inputDFs.get("customerInputDf")

        joinDF=customerInputDf.join(broadcast(list_filter_df),"customer_id")
        return joinDF


# COMMAND ----------

class  AverageTimeDelayTransformer(Transformer):

    def transform(self,inputDFs):
        """
            Find average time delay between buying iphone and Airpods
        """

        transactionInputDf=inputDFs.get("transactionInputDf")

        print("transactionInputDf in AverageTimeDelayTransformer transform class")

        transactionInputDf.show()
        print("data fetching in transform")
        windowspec=Window.partitionBy("customer_id","product_name").orderBy("transaction_date")
        transformDf=transactionInputDf.withColumn("row_number",row_number().over(windowspec)).filter((col("product_name")=="iPhone") | (col("product_name")=="AirPods") & (col("row_number")==1)) 
        
        print("Transformed df in AverageTimeDelayTransformer class with row_num ")
        transformDf.show(truncate=False)
        print("newtransform df in AverageTimeDelayTransformer class")
        window=Window.partitionBy("customer_id").orderBy("transaction_date")
        new_transformdf=transformDf.withColumn("next_transaction_date",lead("transaction_date").over(window)).filter(col("next_transaction_date").isNotNull())\
            .withColumn("delaytime",datediff("next_transaction_date","transaction_date"))

        print("result of newtransform df in AverageTimeDelayTransformer class")
        new_transformdf.show(truncate=False)

        customerInputDf=inputDFs.get("customerInputDf")

        joinDF=customerInputDf.join(broadcast(new_transformdf),"customer_id")
        return joinDF

# COMMAND ----------

