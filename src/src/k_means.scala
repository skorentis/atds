package org.apache.spark.atds

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object k_means {
  
  def main(args: Array[String]) {
   
    Logger.getLogger("org")
    
    val conf = new SparkConf()
    conf.setAppName("k_means")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("/yellow_tripdata_1m.csv")
    
    // Convert to (Coords) RDD[Vector]
    val parsedData = lines.map(s => Vectors.dense(s.split(",")(3).toDouble,s.split(",")(4).toDouble)).cache()
    
    //set first 5 as starting centroids
    val Vector_coords = parsedData.collect()
    var insert = Array(Vector_coords(0),Vector_coords(1),Vector_coords(2),Vector_coords(3),Vector_coords(4))
    val model = new KMeansModel(insert)
    
    val numIterations = 3
    val numClusters = 5
    val clusters = new KMeans()
    .setK(numClusters)
    .setInitialModel(model)
    .setMaxIterations(numIterations)
    .run(parsedData)
    
    val results = clusters.clusterCenters
    var i = 1
    println("Id\t\tCentroid") 
    for (result <- results) {
       println(s"$i\t\t$result")
       i = i + 1
    } 
  }
}