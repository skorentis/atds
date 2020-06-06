package org.apache.spark.atds

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object AverageTaxiTime {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val start_date_time = fields(1).split(" ")
    val end_date_time = fields(2).split(" ")
    val start_time = start_date_time(1).split(":")
    val end_time = end_date_time(1).split(":")
    val start_hour_ = start_time(0)
    val start_hour = start_hour_.toInt
    val start_min = start_time(1).toInt
    val start_sec = start_time(2).toFloat
    var end_hour = end_time(0).toInt
    val end_min = end_time(1).toInt
    val end_sec = start_time(2).toFloat
    if(end_hour < start_hour)
      end_hour += 24 
    val duration = (end_hour - start_hour).toFloat * 60.0f + (end_min - start_min).toFloat + (end_sec - start_sec) / 60.0f
    (start_hour_,duration.toDouble)
  }
   
  def main(args: Array[String]) {
   
    Logger.getLogger("org")
    
    val conf = new SparkConf()
    conf.setAppName("AverageTaxiTime")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("/yellow_tripdata_1m.csv")
    
    // Convert to (start_hour,duration) tuples
    val parsedLines = lines.map(parseLine)
    
    // Now we have (start_hour,(duration,1))
    val mapped = parsedLines.map( x => (x._1,(x._2,1)) )
    
    // Reduce by start_hour , add durations and keep track of amount
    val totalDuration = mapped.reduceByKey( (x,y) => (x._1 + y._1,x._2 + y._2))
    
    // Collect, format, and print the results
    val results = totalDuration.collect()
    println("HourOfDay\t\tAverageTripDuration") 
    for (result <- results.sorted) {
       val hour = result._1
       var dur1 = result._2._1 / result._2._2.toDouble
       dur1 = dur1.toFloat
       val dur = f"$dur1%.14f"
       println(s"$hour\t\t\t$dur") 
    }
      
  }
}