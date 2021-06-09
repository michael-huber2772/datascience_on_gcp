#import print library
import logging

#import apache beam library
import apache_beam as beam

#import pipeline options.
from apache_beam.options.pipeline_options import  PipelineOptions

#Set log level to info
root = logging.getLogger()
root.setLevel(logging.INFO)

#Create a pipeline
plOps = beam.Pipeline(options=PipelineOptions())

#Function to extract Product Type and Price
class ExtractProductTypePrice(beam.DoFn):

  def process(self, element):
        strArray = element.split(',')
        return [(strArray[2],float(strArray[3]))]

#Function to extract Customer Type 
class ExtractCustomerType(beam.DoFn):

  def process(self, element):
        strArray = element.split(',')
        return [(strArray[1],1)]

#function to print the size of a PCollection
def printSize(PColl,PName):
    #Print the number of lines read
    (  PColl
                | 'Counting Lines for  %s' % PName
                    >> beam.CombineGlobally(beam.combiners.CountCombineFn())
                | 'Print Line Count for %s' % PName
                    >> beam.ParDo(lambda c: logging.info('\nTotal Lines in %s = %s \n' , PName,c))  
     )

#--------------------------------------------------
# 1.Read from a text file.
#--------------------------------------------------

#Read the file from Google Cloud Storage
transactions = ( plOps 
                | 'Read Transaction CSV'
                    >> beam.io.ReadFromText('gs://dataflow-test-314414/data/sales_transactions.csv')
                )

printSize(transactions,'Raw Transactions')

#--------------------------------------------------
#2. Extract Product Type and Price from the data using Pardo.
#--------------------------------------------------

#Run a transform to extract the information.

# This function in the ParDo will be run on every record

prodTypePrice = ( transactions 
                | 'Extracting Product Type and Price' 
                    >> beam.ParDo(ExtractProductTypePrice())
                )

#Print size of PCollection
printSize(prodTypePrice,'Product Type and Price')

#--------------------------------------------------
#3. Group prices by Product Type using Group By
#--------------------------------------------------

#Run a transform to collect prices by Product Type

prodTypeGroups = ( prodTypePrice
                | 'Grouping by Product Type'
                    >> beam.GroupByKey()
                )

#Print the PCollection
# ( prodTypeGroups | 'Print prodTypeGroups'
#         >> beam.ParDo( lambda k,v: logging.info('Product Type %s: %s' ,k,v)))
# To get the print to work I changed the code to the following
( prodTypeGroups | 'Print prodTypeGroups'
        >> beam.ParDo( lambda k: logging.info('Product Type %s: %s' ,k[0], k[1])))

#--------------------------------------------------
#4.Find average of Prices by Product Group using Map
#--------------------------------------------------
prodTypeAverage = ( prodTypeGroups
                | 'Average by Product Type'
                >> beam.Map( lambda k: (k[0],sum(k[1])/len(k[1])))
                )


#Print the PCollection
( prodTypeAverage | 'Print prodTypeAverage'
        >> beam.ParDo( lambda k: logging.info('Product Type %s: Average %f' ,k[0],k[1])))


#--------------------------------------------------
#5.Find transactions by Customer Type
#--------------------------------------------------

custTypeCount = ( transactions 
                | 'Extracting Customer Type' 
                    >> beam.ParDo(ExtractCustomerType())
                | 'Summarize Customer Type' 
                    >> beam.CombinePerKey(beam.combiners.CountCombineFn())
                )

#Print the PCollection
( custTypeCount | 'Print Transactions by Customer Type'
        >> beam.ParDo( lambda k: logging.info('Customer Type %s: Count %d' ,k[0], k[1] ))
        )

	
#Write output to a text file
( custTypeCount | 'Write to GS Text'
	>> beam.io.WriteToText('gs://dataflow-test-314414/data/customertype-summary.txt')
)



# Run the pipeline
result = plOps.run()
#  wait until pipeline processing is complete
result.wait_until_finish()

