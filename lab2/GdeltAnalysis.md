# Supercomputing for Big Data : Group 10 :computer: :chart_with_upwards_trend:

Hello everyone! this blog post is a deliverable for assignment 2 of our course "Supercomputing for Big Data" at TU delft. The succeeding topics in this blog discuss about our spark application implementation and its performance analysis.

### Quick Recap 

In Lab assignment 1 report we proposed to measure the performance by finding balance between performance and cost.That is, we intend to do a performance vs cost optimization. How do we do that? We plan to define the metrics as shown in the table below :

| Machine Type  | Cost of a machine/ hour | Number of instances  | time taken to complete the task | Cost of cluster|
| ------------- | ----------------------- | -------------------- | ------------------------------- | -------------- |
| Machine_Type  |         $/hr (x)        | no_of_instances (y)  |            z_hours (z)          |   x.y.z        |

The above performance metrics was pretty easy to define. But is it really effective :confused:?  We will find that out later in this blog :smile:

Following is a code snippet of our dataframe implementation from lab assignment-1 which we have used extensively for performance analysis (reason for selecting DF implementation discussed later) :

###### code 
```
val ds = spark.read
              .schema(schema)
              .option("timestampFormat", "yyyyMMddhhmmss") 
              .option("sep","\t")
              .csv("./data/segment/*.csv")
              .as[GData]
               
val urlCleaner = (s:String) => {
   if (s == null) null else s.replaceAll(","," ").replaceAll("\\d+(?:[.,]\\d+)*\\s*", "")
}

val url_cleaner_udf = udf(urlCleaner)

val filter2=ds.select("Gdate","AllNames")                                                   //select Gdate and AllNames
val a = filter2.withColumn("AllNames",split(col("AllNames"),";").cast("array<String>"))     //split using semicolon
val b = a.withColumn("AllNames",explode($"AllNames"))                                       //explode AllNames
val c = b.withColumn("AllNames", url_cleaner_udf(b("AllNames")))                            //user defined function to remove numbers
val newDS = c.withColumn("Gdate", c("Gdate").cast(DateType))                                // cast timestamp to date
val counts = newDS.groupBy($"Gdate",$"AllNames").agg(count($"AllNames").as("count"))        // count the topic occurence


counts.withColumn("rank", rank().over(Window.partitionBy("Gdate").orderBy($"count".desc)))  //descending order
      .filter($"rank" <= 10)                                                                //select top 10
      .drop("rank").withColumn("NewColumn", struct(counts("AllNames"), counts("count")))    //combine topic and count
      .select($"Gdate",$"NewColumn")
      .groupBy($"Gdate").agg(collect_list($"NewColumn").as("AllNames"))
      .rdd.map(_.toSeq.toList)
      .foreach(println) 
```      
### Before we start...
As discussed in the feedback session for lab assignment-1, User Defined functions(UDF) kill the spark dataframe approach optimizations. Our code consisted a UDF for discarding the unwanted numbers in 'AllNames' column. Hence the changes that were made are: 

```
val c = b.withColumn("AllNames",split(col("AllNames"),",")(0)) 
//removed the UDF
//val c = b.withColumn("AllNames", url_cleaner_udf(b("AllNames"))) //using user defined function to remove the numbers
```

### Naive Start


