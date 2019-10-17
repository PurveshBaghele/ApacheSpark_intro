package group10

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split 
import org.apache.log4j.{Level, Logger}
import java.sql.Timestamp
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, desc}



//SBT command : docker run -it --rm -v "`pwd`":/root hseeberger/scala-sbt sbt -J-XX:MaxMetaspaceSize=700m

object Group10_DF{
	case class GData ( 
				GKGRECORDID: String, 
				Gdate: Timestamp, 
				SourceCollectionIdentifier: Integer,
				SourceCommonName: String,
				DocumentIdentifier: String,
				Counts: String,
				V2Counts: String,
				Themes: String,
				V2Themes: String,
				Locations: String,
				V2Locations: String,
				Persons: String,
				V2Persons: String,
				Organizations: String,
				V2Organizations: String,
				V2Tone: String,
				Dates: String,
				GCAM: String,
				SharingImage: String,
				RelatedImages: String,
				SocialImageEmbeds: String,
				SocialVideoEmbeds: String,
				Quotations: String,
				AllNames: String,
				Amounts: String,
				TranslationInfo: String,
				Extras: String 
			  )

	def main(args : Array[String]){
			
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
			val schema = StructType( 
					Array( StructField("GKGRECORDID", StringType), 
						   StructField("Gdate", TimestampType), 
						   StructField("SourceCollectionIdentifier", IntegerType), 
						   StructField("SourceCommonName", StringType), 
						   StructField("DocumentIdentifier", StringType), 
						   StructField("Counts", StringType), 
						   StructField("V2Counts", StringType), 
						   StructField("Themes", StringType), 
						   StructField("V2Themes", StringType), 
						   StructField("Locations",StringType), 
						   StructField("V2Locations",StringType), 
						   StructField("Persons",StringType), 
						   StructField("V2Persons",StringType), 
						   StructField("Organizations",StringType), 
						   StructField("V2Organizations",StringType), 
						   StructField("V2Tone", StringType), 
						   StructField("Dates",StringType), 
						   StructField("GCAM", StringType), 
						   StructField("SharingImage", StringType), 
						   StructField("RelatedImages",StringType), 
						   StructField("SocialImageEmbeds",StringType), 
						   StructField("SocialVideoEmbeds",StringType), 
						   StructField("Quotations", StringType),
						   StructField("AllNames", StringType), 
						   StructField("Amounts",StringType), 
						   StructField("TranslationInfo",StringType), 
						   StructField("Extras", StringType)
					)
				  )
				  	  
				val spark = SparkSession
							.builder
							.appName("Group10")
							.config("spark.master", "local") //comment for running on cluster
							.getOrCreate()
				val sc = spark.sparkContext
				import spark.implicits._

				//val t2 = System.nanoTime
				val ds = spark.read 
							  .schema(schema)
							  .option("timestampFormat", "yyyyMMddhhmmss") 
							  .option("sep","\t")
							  .csv(args(0))
							  .as[GData]
							  //./data/segment/20150218230000.gkg.csv

				
			// val urlCleaner = (s:String) => {
			//    if (s == null) null else s.replaceAll(","," ").replaceAll("\\d+(?:[.,]\\d+)*\\s*", "")
			// }

			//val url_cleaner_udf = udf(urlCleaner)
            
			val filter2=ds.select("Gdate","AllNames")  //select Gdate and AllNames
			val a = filter2.withColumn("AllNames",split(col("AllNames"),";").cast("array<String>")) //split using semicolon
            val b = a.withColumn("AllNames",explode($"AllNames")) //explode AllNames
			val c = b.withColumn("AllNames",split(col("AllNames"),",")(0)) 
            //val c = b.withColumn("AllNames", url_cleaner_udf(b("AllNames"))) //using user defined function to remove the numbers
            val newDS = c.withColumn("Gdate", c("Gdate").cast(DateType)) // cast timestamp to date
            val counts = newDS.groupBy($"Gdate",$"AllNames").agg(count($"AllNames").as("count")) // count the topic occurence
			
            counts.withColumn("rank", rank().over(Window.partitionBy("Gdate").orderBy($"count".desc))) //descending order
                .filter($"rank" <= 10) // select top 10
                .drop("rank").withColumn("NewColumn", struct(counts("AllNames").as("topic"), counts("count")))// combine topic and count
                .select($"Gdate",$"NewColumn")
                .groupBy($"Gdate").agg(collect_list($"NewColumn").as("AllNames"))
                //.rdd.map(_.toSeq.toList)
				//.coalesce(1)
				.write.format("json").save(args(1))
                //.saveAsTextFile(args(1))
			
           // val duration2 = (System.nanoTime - t2) / 1e9d
			//println("rdd implementation execution time: "+duration2)
				
				
			/*
				To save into a json file
				
				counts.withColumn("rank", rank().over(Window.partitionBy("Gdate").orderBy($"count".desc)))
							.filter($"rank" <= 10)
							.drop("rank")
							.write
							.mode("append")
							.json("E:\\supercomputigLab\\SBD-tudelft\\lab1\\data\\segment\\report.json")
			*/			
			
			spark.stop()
		}
	}
 

/*OUTPUT
List(2015-02-19, WrappedArray([United States ,1497], [Islamic State ,1233], [New York ,1058], [United Kingdom ,735], [White House ,723], [Los Angeles ,620], [New Zealand ,590], [Associated Press ,498], [San Francisco ,479], [Practice Wrestling Room ,420]))
List(2015-02-18, WrappedArray([Type ParentCategory ,2290], [Islamic State ,1787], [United States ,1210], [New York ,727], [White House ,489], [Los Angeles ,424], [Associated Press ,385], [New Zealand ,353], [United Kingdom ,325], [Jeb Bush ,298]))
rdd implementation execution time: 45.668898278
*/