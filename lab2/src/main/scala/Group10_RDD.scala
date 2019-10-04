package group10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split 
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._



object Group10{
	
  def main(args: Array[String]) {

	Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
	
	val spark = SparkSession
      .builder
      .appName("Group10")
      //.config("spark.master", "local")
      .getOrCreate()
	  
	val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

val t2 = System.nanoTime          

// val grdd=sc.textFile("./data/segment/") //read all the csv files
val grdd=sc.textFile(args(0)) //read all the csv files

val transform1=grdd.map(x=>x.split("\t")).filter(col=>col.size>23 && col(23)!="") //filter entries having allNames column null

// val rdd1= transform1.map(col=>(col(1).substring(0, 4)+"-"+col(1).substring(4, 6) +"-"+ col(1).substring(6, 8),col(23).split(";")))// map date and AllNames
//                           .mapValues(t=>t.map(s=>(s.split(",")(0),1)))// split allNames without distinct and map (topics with 1)
//                           .reduceByKey((x,y)=>x ++ y) //ReduceByKey on date
//                           .map(x => {(x._1, x._2.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(t=>t._2).reverse.take(10))}) //count topics then sort and reverse
//                           //.collect().foreach(col=>println(col._1,col._2.mkString(" "))) //print the RDD





// val rdd2= transform1.map(col=>(col(1).substring(0, 4)+"-"+col(1).substring(4, 6) +"-"+ col(1).substring(6, 8),col(23).split(";")))// map date and AllNames
//                           .mapValues(t=>t.map(s=>(s.split(",")(0),1)).distinct)// split allNames with distinct and map (topics with 1)
//                           .reduceByKey((x,y)=>x ++ y) //ReduceByKey on date
//                           .map(x => {(x._1, x._2.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(t=>t._2).reverse.take(10))}) //count topics then sort and reverse
//                           .collect().foreach(col=>println(col._1,col._2.mkString(" "))) //print the rdd

val rdd2= transform1.map(col=>(col(1).substring(0, 4)+"-"+col(1).substring(4, 6) +"-"+ col(1).substring(6, 8),col(23).split(";")))// map date and AllNames
                          .mapValues(t=>t.map(s=>(s.split(",")(0),1)))// split allNames with distinct and map (topics with 1)
                          .reduceByKey((x,y)=>x ++ y) //ReduceByKey on date
                          .map(x => {(x._1, x._2.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(t=>t._2).reverse.take(10))}) //count topics then sort and reverse
                          .map(col=>(col._1,col._2.mkString(" ")))
                          //.coalesce(1)
                          .saveAsTextFile(args(1)) //print the rdd
                          // .saveAsTextFile("s3://awsgroup10/newfile.txt")
                          //.saveAsTextFile("./data/newfile.txt")

// val rdd2= transform1.map(col=>(col(1).slice(0,8),col(23).split(";")))// map date and AllNames
//                           .mapValues(t=>t.map(s=>(s.split(",")(0),1)))// split allNames with distinct and map (topics with 1)
//                           .reduceByKey((x,y)=>x ++ y) //ReduceByKey on date
//                           .map(x => {(x._1, x._2.groupBy(_._1).mapValues(_.map(_._2).sum).toArray.sortBy(t=>t._2).reverse.take(10))}) //count topics then sort and reverse
//                           .foreach(col=>println(col._1,col._2.mkString(" ")))
                          //.coalesce(1)
                          //.saveAsTextFile(args(1)) //print the rdd
                          // .saveAsTextFile("s3://awsgroup10/newfile.txt")
                         // .saveAsTextFile("./data/newfile.txt")

// val duration2 = (System.nanoTime - t2) / 1e9d
// println("rdd implementation execution time: "+duration2)

//spark.stop()
    
  }
}

 

  /*OUTPUT
  // without Distinct
  (2015-02-18,(Type ParentCategory,2290) (Islamic State,1787) (United States,1210) (New York,727) (White House,489) (Los Angeles,424) (Associated Press,385) (New Zealand,353) (United Kingdom,325) (Jeb Bush,298))
  (2015-02-19,(United States,1497) (Islamic State,1233) (New York,1058) (United Kingdom,735) (White House,723) (Los Angeles,620) (New Zealand,590) (Associated Press,498) (San Francisco,479) (Practice Wrestling Room,420))
  
  //with Distinct
  (2015-02-18,(United States,843) (New York,521) (Islamic State,459) (Associated Press,300) (Los Angeles,286) (White House,281) (United Kingdom,211) (Middle East,172) (New Zealand,170) (President Barack Obama,169))
  (2015-02-19,(United States,1025) (New York,756) (Islamic State,461) (United Kingdom,425) (Associated Press,421) (White House,403) (Los Angeles,395) (New Zealand,297) (San Francisco,262) (President Barack Obama,253))

*/



