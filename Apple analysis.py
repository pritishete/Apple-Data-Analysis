# Databricks notebook source
# MAGIC %run "./Reader_factory"
# MAGIC

# COMMAND ----------

# MAGIC %run "./Transform"
# MAGIC

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

class FirstWorkflow:

    """
    ETL pipeline to generate the data for all customers who have bought Airpods after buying iphone
    """

    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source

        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #step2:Implement Transformation Logic
        #Customers who bought Airpods after buying the iphone
        #LEAD(product_name)-> Partition by :customer_id and orderBy transaction_date asc
        #nextProductName

        firstTransformedDF=AirpodsAfterIphoneTransformer().transform(inputDFs)
        print("Customer details who bought Airpods after buying iphone")
        firstTransformedDF.select("customer_id","customer_name","location").show()

        #step3:Load all required data into different sink

        AirpodsAfterIphoneLoader(firstTransformedDF).sink()
        print("Data has loaded successfully")













# COMMAND ----------

class SecondWorkflow:

    """
    ETL pipeline to generate the data for all customers who have bought only Airpods and iphone
    """

    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source

        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #step2:Implement Transformation Logic
        #Customers who bought only Airpods and  iphone
        

        OnlyAirpodsAndIphoneDF=OnlyAirpodsAndIphone().transform(inputDFs)
        print("Customer details who bought only Airpods and  iphone")
        OnlyAirpodsAndIphoneDF.select("customer_id","customer_name","location").show()

        #step3:Load all required data into different sink

        OnlyAirpodsAndIphoneLoader(OnlyAirpodsAndIphoneDF).sink()
        print("Data has loaded successfully")













# COMMAND ----------

class ThirdWorkflow:

    """
    ETL pipeline to list out all the products brought by customer after their initial purchase
    """

    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source

        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #step2:Implement Transformation Logic
        #list out all the products brought by customer after their initial purchase
        

        ListProductsAfterInitialPurchaseDF=ListAllProducts().transform(inputDFs)
        print("List of all products bought by customer after initial purchase")
        ListProductsAfterInitialPurchaseDF.select("customer_id","customer_name","location","product_list").show()

        #step3:Load all required data into different sink

        ListProductsAfterInitialLoader(ListProductsAfterInitialPurchaseDF).sink()
        print("Data has loaded successfully")


# COMMAND ----------

class FourthWorkflow:

    """
    ETL pipeline to  find the average time delay between buying iphone and airpods for each customer
    """

    def __init__(self):
        pass

    def runner(self):

        # Step 1: Extract all required data from different source

        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #step2:Implement Transformation Logic
        # Logic to  find the average time delay between buying iphone and airpods for each customer
        #LEAD(product_name)-> Partition by :customer_id and orderBy transaction_date asc
        #nextProductName

        averageTransformedDF=AverageTimeDelayTransformer().transform(inputDFs)
        print("The average time delay between buying iphone and airpods for each customer")
        averageTransformedDF.select("customer_id","customer_name","delaytime").show()

        #step3:Load all required data into different sink

        AverageDelayTimeLoader(averageTransformedDF).sink()
        print("Data has loaded successfully")

# COMMAND ----------

class WorkflowRunner:

    def __init__(self,name):
        self.name=name


    def runner(self):
        if self.name=="FirstWorkflow":
            return FirstWorkflow().runner()
        elif self.name=="SecondWorkflow":
            return SecondWorkflow().runner()
        elif self.name=="ThirdWorkflow":
            return ThirdWorkflow().runner()
        elif self.name=="FourthWorkflow":
            return FourthWorkflow().runner()
        else:
            return ValueError(f"Not implemented for {self.name}")


name="FourthWorkflow" 
workflowrunner=  WorkflowRunner(name).runner()



# COMMAND ----------

