package org.apache.spark.atds

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object Mult  {
   
  def parseLine(line:String)= {
    val fields = line.split(",")
    val i = fields(0).toInt
    val j = fields(1).toInt
    val value = fields(2).toInt
    (i,j,value)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org")
    
    val conf = new SparkConf()
    conf.setAppName("Mult")
    val sc = new SparkContext(conf)

    // Read each line of input data
    val lines = sc.textFile("/A.csv")
    val lines2 = sc.textFile("/B.csv")
    
    val A = lines.map(parseLine)
    val B = lines2.map(parseLine)
    
    val matrixA = A.map(x => (x._2,(x._1,x._3)))
    val matrixB = B.map(x => (x._1,(x._2,x._3)))
    
    val product1 = matrixA.join(matrixB)
    .map( x  => ((x._2._1._1, x._2._2._1), x._2._1._2*x._2._2._2) )
    
    val product2 = product1.reduceByKey((x,y) => x + y)
    
    val finalproduct = product2.map((x) => (x._1._1, x._1._2, x._2))
    
    val results = finalproduct.collect()    

     for (result <- results.sorted) {
       val i = result._1
       val j = result._2
       val value = result._3
       println(s"$i,$j,$value") 
    }
  }
}