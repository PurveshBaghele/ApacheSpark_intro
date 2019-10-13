# Supercomputing for Big Data : Group 10 :computer: :chart_with_upwards_trend:

*Author 1: Pradyot Patil*

*Author 2: Purvesh Baghele*

Hello everyone! this blog post is a deliverable for assignment 2 of our course "Supercomputing for Big Data" at TU delft. The succeeding topics in this blog discuss about our spark application implementation and its performance analysis.

### Quick Recap 

In Lab assignment 1 report we proposed to measure the performance by finding balance between performance and cost.That is, we intend to do a performance vs cost optimization. How do we do that? We plan to define the metrics as shown in the table below :

| Machine Type  | Cost of a machine/ hour | Number of instances  | time taken to complete the task | Cost of cluster|
| ------------- | ----------------------- | -------------------- | ------------------------------- | -------------- |
| Machine_Type  |         $/hr (x)        | no_of_instances (y)  |            z_hours (z)          |   x.y.z        |

The cost of cluster considers both, the time taken by it and of machine bieng used. The goal is to get a cluster which does justice to time and cost both. The above performance metrics was pretty easy to define. But is it really effective :confused:?  We will find that out later in this blog :smile:

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
As discussed in the feedback session for lab assignment-1, User Defined functions(UDF) kill the spark dataframe approach optimizations. Our [code](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/lab2/GdeltAnalysis.md#code) consisted a UDF for discarding the unwanted numbers in 'AllNames' column. Hence, we removed the UDF as follows: 

```
val c = b.withColumn("AllNames",split(col("AllNames"),",")(0)) 
//removed the UDF
//val c = b.withColumn("AllNames", url_cleaner_udf(b("AllNames"))) //using user defined function to remove the numbers
```

### Naive Start :hatching_chick:

Before we run our spark application on the entire dataset, we wanted to filter down whether we should use our rdd or dataframe approach. We started with small number of instances of m4.large machines and then increased the instances gradually. We compared our RDD and DF implementation on m4.large machines.

##### RDD vs DF

The difference in execution time between RDD and Dataframe approach was 16 seconds for 864 segments which increased upto 56 seconds for 2974 segments. This difference seems trivial for now but when the application scales up to run all the segments of the dataset, this difference will become huge. Also as stated by us in lab assignment-1, we expected the Dataframe approach to be faster as it is compiled into execution plan and then executed. Therefore, spark can optimize the execution plan to reduce execution time.  To still bolster our assumption we ran both our approaches on m4.large machines with 21 instances on whole dataset(4.1TB).

### Minimum Requirement
As mentioned in assignment manual the minimum requirement for assignment is to run the whole dataset on 20 instances of c4.8xlarge machines.
In this section we present the performance of our implementation on 20 instances of c4.8xlarge machines (excluding 1 master node).

![Too much talk](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/images/trump_meme.gif)

##### Lets Start....
Since it was our first go, We slowly scaled the number of segments and machines. Then the two rookie authors of this blog thought they were ready to face the test and took the bold step of trying out the whole dataset on 20 c4.8xlarge core nodes.  Obviously we met our doom.

![Executor Memory Error](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/images/c4.8x%205%20errors/2019-10-12%20(1).png)

##### Bottleneck-1 : executor memory
The executors in the cluster were running out of memory. So we increased the executor and driver memory in the spark configuration. The new configuration were : ` --executor-memory 3g` and `--driver-memory 3g`. We increased the memory by 1gb starting from 1gb and it worked for 3gb :man_shrugging:.

![c4.8xlarge performance](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/images/c4.8x%2020%20success/c4.8xlarge_naive.png) 

Average core node  CPU usage just above 30% :frowning_face: :
![core node CPU usage](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/images/c4.8x%2020%20success/performance.png)

Same case with cluster CPU usage :
![cluster node CPU usage](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/images/c4.8x%2020%20success/cluster_usage.png)

After the naive run, we thought if we can improve the performance by tweaking the spark configuration :thought_balloon: ? 

### Lets dive into spark configuration
In the naive implementation we naively set executor memory to 3gb without any basis. In this section we explain how we got the best(according to us) performance from the cluster by tweaking spark configuration. The AWS [blog](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/) helped us in configuring spark.

We ran multiple experiments on whole dataset 5 with instances of C4.8xlarge machines to find the right amount of `spark.executor.cores` as shown table below :

| executor.cores | executor.memory         | time taken to complete the task (Approx.) | Average CPU Utilization (Approx.)|
| -------------- | ----------------------- | ----------------------------------------- | -------------------------------- | 
| 2              |         3g              | 23 minutes                                | 58%                              |
| 3              |         5g              | 25 minutes                                | 56%                              |
| 4              |         6g              | 18 minutes                                | 66%                              |
| 5              |         8g              | 24 minutes                                | 55%                              |
| 7              |         12g             | 28 minutes                                | 51%                              |

Clearly 4 executor cores suit the implementation. But how did we come up with executor memory?

C4.8xlarge machine has **36vCores** and **60GB** memory. 
