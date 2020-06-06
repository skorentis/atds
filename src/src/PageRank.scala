package org.apache.spark.atds

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object PageRank {
  
  def parseLine(line:String)= {
    val fields = line.split("\t")
    val node = fields(0).toString
    val outbound = fields(1).toString
    (node,outbound)
  }
  
  def main(args: Array[String]) {

    Logger.getLogger("org")
    
    val conf = new SparkConf()
    conf.setAppName("PageRank")
    val sc = new SparkContext(conf)

    // Read each line of input data
    val lines = sc.textFile("/web-Google.txt")
    
    val edges = lines.map(parseLine).groupByKey().cache()
    val numOfIter = 5
    
    var ranks = edges.mapValues(v => 0.5)
    val const = 0.15 / 875713 
 
    for (i <- 1 to numOfIter) {
      val contribs = edges.join(ranks).values
      .flatMap{ 
        case (urls, rank) => 
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      
      ranks = contribs.reduceByKey(_ + _).mapValues(const + 0.85 * _)

    }

    val result = ranks.collect()
    println("NodeId\t\tPageRank")     
    result.foreach(tup => println(tup._1 + "\t\t" + tup._2))
       
  }
}