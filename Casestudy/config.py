# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 22:58:14 2023

@author: anike
"""
def load_csv_data_to_df(spark, file_path):
    """
    Read CSV data
        :atrib_1 spark: spark instance
        :atrib_2 file_path: path to the csv file
        :return  dataframe
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)

def save_output(df, file_path, write_format):
    """
    Write data frame to csv
    atrib_1: df: dataframe
    :param file_path: output file path
    :param write_format: Write file format
    :return: None
    """
    
    # df = df.coalesce(1)
    
    #df.write.mode('overwrite').csv(file_path,header = True)
    #df.write.format(write_format).mode('append').option("header", "true").save(file_path)
    
    df_pand = df.toPandas()
    df_pand.to_csv(file_path, header=True, index = False)
   


FORMAT = {
  'Input_Format' : 'csv',
  'Output_Format': 'csv'
  }
  
INPUT_PATH= { 
  "Charges": "Data/Charges_use.csv",
  "Damages": "Data/Damages_use.csv",
  "Endorse": "Data/Endorse_use.csv",
  "Primary_Person": "Data/Primary_Person_use.csv",
  "Units": "Data/Units_use.csv",
  "Restrict": "Data/Restrict_use.csv" 
  }

OUTPUT_PATH = { 
  1: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/1.csv",
  2: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/2.csv",
  3: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/3.csv",
  4: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/4.csv",
  5: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/5.csv",
  6: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/6.csv",
  7: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/7.csv",
  8: "C:/Users/anike/Desktop/aniket interviews/BCG/Casestudy/Output/8.csv"
  }

