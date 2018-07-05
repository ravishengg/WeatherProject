package com.ravz.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql._


object Project {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        val conf = new SparkConf()
    
    // Create a SparkContext using every core of the local machine, named RatingsCounter
 val sc = new SparkContext("local[*]", "Project")
   
        
        val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
//read weather files and airports.dat file
  val weather = spark.read.format("csv").option("header","true").option("ignoreLeadingWhiteSpace", "true").load("../weather/")
    val loc = spark.read.format("csv").option("header","false").option("ignoreLeadingWhiteSpace", "true").load("../airport/")
    
    weather.registerTempTable("weather")
    loc.registerTempTable("loc")
    
    val weatherdf = spark.sqlContext.sql("select name,local_date_time_full,cast(lat as decimal(10,1)) as lat,cast(lon as decimal(10,1)) as lon,"+
    " cast(apparent_t as decimal(10,2)) as avg_temp,cast(press_qnh as decimal(10,2)) as pressure ,cast(rel_hum as decimal(10,2)) as humidity from  weather")
    
    weatherdf.registerTempTable("weather2")
    
    //If humidity > 100 it will precipitate, if temp > 0 it will rain and if temp < 0 it will snow
    val weatherdf2 = spark.sqlContext.sql("select name,local_date_time_full,lat,lon,"+
  " case when (humidity > 100 and avg_temp > 0) then \"Rain\" "+
       "when (humidity > 100 and avg_temp <= 0) then \"Snow\" "+
	   "else \"Sunny\" end as Climate, "+
  " avg_temp,pressure ,humidity  from  weather2")
    
 weatherdf2.registerTempTable("Climate")
 
 val loc2 =  spark.sqlContext.sql("Select _c4 as Code,cast(_c6 as decimal(10,1)) as lat,cast(_c7 as decimal(10,1)) as lon from loc")
 
 loc2.registerTempTable("Location")
 
 val resultdf = spark.sqlContext.sql("Select concat(l.code,\"|\",cast(c.lat as char(6)),\",\",cast(c.lon as char(6)),\"|\",c.local_date_time_full,\"|\",c.climate,\"|\",cast(c.avg_temp as char(8)),\"|\",cast(c.pressure as char(8)),\"|\",cast(c.humidity as char(8))) as dummy from "+    
  " climate c left outer join location l ON "+
     " l.lat = c.lat and l.lon = c.lon ")
     
    val rowsrdd = resultdf.rdd
  
  
  rowsrdd.saveAsTextFile("../result")
        
  }
}
