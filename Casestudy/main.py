# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from pyspark.sql import functions as F
import config as cfg



class Accidents:
    def __init__(self):
        file_paths = cfg.INPUT_PATH
        self.df_charges = cfg.load_csv_data_to_df(spark, file_paths["Charges"])
        self.df_charges.createOrReplaceTempView("charges")
        self.df_damages = cfg.load_csv_data_to_df(spark, file_paths["Damages"])
        self.df_endorse = cfg.load_csv_data_to_df(spark, file_paths["Endorse"])
        self.df_primary_person = cfg.load_csv_data_to_df(spark, file_paths["Primary_Person"])
        self.df_primary_person.createOrReplaceTempView("Primary_Person")
        self.df_units = cfg.load_csv_data_to_df(spark, file_paths["Units"])
        self.df_restrict = cfg.load_csv_data_to_df(spark, file_paths["Restrict"])
        
  
        
    def male_accidents(self, output_path, output_format):
        
        """
        Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
        :param output_path: output file path
        :param output_format: Write file format
        :return: dataframe count
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == 'MALE')
        df = df.dropDuplicates(["crash_id"]).select("crash_id").select("crash_id")
        #print("after",df.count())
        cfg.save_output(df,cfg.OUTPUT_PATH[1],cfg.FORMAT['Output_Format'])
        
        return df.count()
    
    def two_wheeler_accidents(self, output_path, output_format):
        """
        Analysis 2: How many two wheelers are booked for crashes? 
        :param output_format: Write file format
        :param output_path: output file path
        :return: dataframe count
        """
        df = self.df_units.filter(self.df_units.VEH_BODY_STYL_ID == 'MOTORCYCLE')
        df = df.dropDuplicates(["crash_id"]).select(["CRASH_ID"])#,"veh_body_styl_id"])
        #df.show()
        cfg.save_output(df,cfg.OUTPUT_PATH[2],cfg.FORMAT['Output_Format'])
        return df.count()
    
    
    def highest_female_accident_state(self, output_path, output_format):
         
        """
        Analysis 3: Which state has highest number of accidents in which females are involved? 
        :param output_format: Write file format
        :param output_path: output file path
        :return: state name with highest female accidents
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == 'FEMALE')
        df = df.groupBy('DRVR_LIC_STATE_ID').count().orderBy(col("count").desc())
        cfg.save_output(df,cfg.OUTPUT_PATH[3],cfg.FORMAT['Output_Format'])
        return df.first().DRVR_LIC_STATE_ID
    
    
    def top_5_15_injury_causing_make(self, output_path, output_format):
        """
        Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        :param output_format: Write file format
        :param output_path: output file path
        :return: Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        
        df = self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA"). \
            withColumn("TOT_INJURY",self.df_units.TOT_INJRY_CNT+self.df_units.DEATH_CNT).\
            groupBy('VEH_MAKE_ID').sum('TOT_INJURY').orderBy(col('sum(TOT_INJURY)').desc())
        df = df.limit(15).subtract(df.limit(4))   
        cfg.save_output(df,cfg.OUTPUT_PATH[4],cfg.FORMAT['Output_Format'])
        return [veh_make[0] for veh_make in df.select("VEH_MAKE_ID").collect()]

    
    def top_ethnic_grp_for_each_body_style(self, output_path, output_format):        
        """
        Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style          
        :param output_format: Write file format
        :param output_path: output file path
        :return: None
        """
        
        df_units = self.df_units.filter(self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"]) == False)
        df_primary_person = self.df_primary_person.filter(self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"]) == False)
        
        df = df_units.join(df_primary_person,\
                                df_units.CRASH_ID==df_primary_person.CRASH_ID,\
                                "inner").\
            groupby(["VEH_BODY_STYL_ID",'PRSN_ETHNICITY_ID']).count().orderBy('VEH_BODY_STYL_ID')
        df = df.withColumn('RN',row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col('count').desc()) )  ).\
            filter(col("RN")==1).select(["VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID"])  
        
        df.show(truncate=False)
        cfg.save_output(df,cfg.OUTPUT_PATH[5],cfg.FORMAT['Output_Format'])
        return "Table displayed above"
    
    
    def top_5_zip_cd_with_alcohol_for_crash(self, output_path, output_format):
        """
        Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of Zip Codes
        """
        
        under_alcohol = ["UNDER INFLUENCE - ALCOHOL","HAD BEEN DRINKING"]
        # df = self.df_units.filter(self.df_units.CONTRIB_FACTR_1_ID.isin(under_alcohol)).filter(self.df_units.CONTRIB_FACTR_P1_ID.isin(under_alcohol))
        # df_new = df.join(self.df_primary_person, on = ["CRASH_ID"],how="inner").\
        #         dropna(subset=["DRVR_ZIP"]).\
        #         groupBy("DRVR_ZIP").count().orderBy(col('count').desc())
        # df_new.show(1)
        
        df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").isin(under_alcohol) | col("CONTRIB_FACTR_2_ID").isin(under_alcohol)).\
            groupby("DRVR_ZIP").count().orderBy(col("count").desc())
        df.show(6)
        cfg.save_output(df,cfg.OUTPUT_PATH[6],cfg.FORMAT['Output_Format'])
        return [zip_cd[0] for zip_cd in df.limit(5).collect()]
    
    def no_damage_avail_inc_crash_ids(self, output_path, output_format):
        """
        Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        :param output_format: Write file format
        :param output_path: output file path
        :return: List of crash ids
        """
        df = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner')
        df = df.filter((df.DAMAGED_PROPERTY.contains("NO DAMAGE")) | (df.DAMAGED_PROPERTY.contains("NONE")) )
        df=df.filter( (F.regexp_extract("VEH_DMAG_SCL_1_ID", "([0-9]+)",1) > 4) | \
                       (F.regexp_extract("VEH_DMAG_SCL_2_ID", "([0-9]+)",1) > 4) ).\
             filter(df.FIN_RESP_TYPE_ID.contains("INSURANCE")).\
             select(["CRASH_ID","DAMAGED_PROPERTY","VEH_DMAG_SCL_1_ID","VEH_DMAG_SCL_2_ID","FIN_RESP_TYPE_ID"])
        df.show(5)
        cfg.save_output(df,cfg.OUTPUT_PATH[7],cfg.FORMAT['Output_Format'])
        return len(df.collect())#[crash_id[0] for crash_id in df.collect()]
    
    def top_5_brand_for_speeding(self, output_path, output_format):
        """
        8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        :param output_format: Write file format
        :param output_path: output file path
        :return List of Vehicle brands
        """
        licence = ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
        
        top_25_states = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        
        top_10_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]
        
        df_units =self.df_units.filter(self.df_units.VEH_COLOR_ID.isin(top_10_colors)).\
                                filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_states))
                                
        df = self.df_charges.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(df_units, on=['CRASH_ID'], how='inner'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(licence)).\
            select(["CRASH_ID","VEH_MAKE_ID","VEH_LIC_STATE_ID","VEH_COLOR_ID"])
        df_new = df.groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc())
        df_new.show(5)
        cfg.save_output(df_new,cfg.OUTPUT_PATH[8],cfg.FORMAT['Output_Format'])
        return [veh_make[0] for veh_make in df_new.select("VEH_MAKE_ID").limit(5).collect()]


if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("USVehicleAccidentAnalysis") \
        .getOrCreate()

    
   
    
    
    spark.sparkContext.setLogLevel("ERROR")
    print('started')
   
    Accidents = Accidents()
    
    print("1. Find the number of crashes (accidents) in which number of persons killed are male?")
    print("1. ANSWER: ",Accidents.male_accidents(cfg.OUTPUT_PATH[1], cfg.FORMAT["Output_Format"]))
    print()
    print("2. How many two wheelers are booked for crashes? ")
    print("2. ANSWER: ",Accidents.two_wheeler_accidents(cfg.OUTPUT_PATH[2], cfg.FORMAT["Output_Format"]))
    print()
    print("3. Which state has highest number of accidents in which females are involved?  ")
    print("3. ANSWER:", Accidents.highest_female_accident_state(cfg.OUTPUT_PATH[3], cfg.FORMAT["Output_Format"]))
    print()
    print("4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
    print("4. ANSWER:", Accidents.top_5_15_injury_causing_make(cfg.OUTPUT_PATH[4], cfg.FORMAT["Output_Format"]))
    print()
    print("5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style ")
    print("5. ANSWER:", Accidents.top_ethnic_grp_for_each_body_style(cfg.OUTPUT_PATH[5], cfg.FORMAT["Output_Format"]))
    print()
    print("6. Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash")
    print("6. ANSWER:", Accidents.top_5_zip_cd_with_alcohol_for_crash(cfg.OUTPUT_PATH[6], cfg.FORMAT["Output_Format"]))
    print()
    print("7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance")
    print("7. ANSWER:", Accidents.no_damage_avail_inc_crash_ids(cfg.OUTPUT_PATH[7], cfg.FORMAT["Output_Format"]))
    print()
    print("8. Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences")
    print("8. ANSWER:", Accidents.top_5_brand_for_speeding(cfg.OUTPUT_PATH[8], cfg.FORMAT["Output_Format"]))
    print("endeddddddd")
    