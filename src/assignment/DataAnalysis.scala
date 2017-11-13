/*
 * Using spark-sql, Find:
1. What are the total number of gold medal winners every year
2. How many silver medals have been won by USA in each sport
 * 
 */

package assignment


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.udf

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 19.2").setMaster("local")
    
    //setting the configuration and creating an instance of SparkContext 
    val sc = new SparkContext(config)
    
    //Entry point of our sqlContext
    val sqlContext = new HiveContext(sc)
    
    //to use toDF method 
    import sqlContext.implicits._

    //Scheam for sports_data table
    val sports_schema = StructType(List (
    StructField("firstname",StringType,true),
    StructField("lastname",StringType,false),
    StructField("sports",StringType,false),
    StructField("medal_type",StringType,false),
    StructField("age",IntegerType,false),
    StructField("year",IntegerType,false ),
     StructField("country",StringType,false )
    ))
    
    //Create RDD from textFile
    val sports_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/sports_data")
    
    //Removing the column names from the rdd 
    val noHeader = sports_file.mapPartitionsWithIndex( 
    (i, iterator) => 
    if (i == 0 && iterator.hasNext) { 
       iterator.next 
       iterator 
    } else iterator) 
     
    //Store the columns in the Row case class
    val sports_rowsRDD = noHeader.map{lines => lines.split(",")}.map{col => Row(col(0),col(1),col(2),col(3),col(4).toInt,col(5).toInt,col(6).trim)}
    
    //Create the dataframe
    val sportsDF = sqlContext.createDataFrame(sports_rowsRDD, sports_schema)
    
    //Create a temp tample which can be used for using sql statement to query from
    sportsDF.registerTempTable("sports_data")
    
    //select all data from table
    val dataDF = sqlContext.sql("select * from sports_data")
    
    // Total number of gold medal winners every year
    val total_gold_medals = sqlContext.sql("select count(firstname),year,medal_type from sports_data group by year,medal_type having medal_type='gold'")
    
    total_gold_medals.show
    
    //silver medals have been won by USA in each sport
    val silver_medals = sqlContext.sql("select count(sports) as count_of_medals,sports,medal_type,country from sports_data group by sports,medal_type,country having medal_type='silver' and country='USA'")
    
    silver_medals.show
  }
}