package org.apache.spark.atds

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxAmountPaid {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val id = fields(0).toString
    val cost = fields(7).toFloat
    (id,cost)
  }
  
  def parseLine2(line:String)= {
    val fields = line.split(",")
    val id = fields(0).toString
    val vendorid = fields(1).toInt
    (id,vendorid)
  }
  
  def main(args: Array[String]) {
   
    Logger.getLogger("org")
    
    val conf = new SparkConf()
    conf.setAppName("MaxAmountPaid")
    val sc = new SparkContext(conf)
    
    // Read each line of input data
    val lines = sc.textFile("/yellow_tripdata_1m.csv")
    
    //(route_id,cost)
    val costs_ids = lines.map(parseLine)
    
    val lines2 = sc.textFile("/yellow_tripvendors_1m.csv")
    //(route_id,vendor_id)
    val vendorsid = lines2.map(parseLine2)
    
    // JOIN on route_id
    val mapped1 = costs_ids.join(vendorsid)
    
    //(vendor_id,cost_of_a_route)
    val mapped = mapped1.map(x => (x._2._2, x._2._1))
    
    // Reduce by vendor_id and keep max cost
    val totalDuration = mapped.reduceByKey( (x,y) => max(x,y))
    
    //so now we have (vendor_id,route_with_max_cost)
    // Collect, format, and print the results
    val results = totalDuration.collect()
    
    println("VendorID\t\tMaxAmountPaid") 
    for (result <- results.sorted) {
       val id = result._1
       val cost = result._2
       val cost_ = f"$cost%.2f"
       println(s"$id\t\t\t$cost_") 
    }
      
  }
}