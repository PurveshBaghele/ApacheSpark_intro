# Supercomputing for Big Data : Group 10 :computer: :chart_with_upwards_trend:

*Author 1: Pradyot Patil*

*Author 2: Purvesh Baghele*

Hello everyone! this blog post is a deliverable for assignment 2 of our course "Supercomputing for Big Data" at TU delft. The succeeding topics in this blog discuss about our spark application implementation and its performance analysis.

### Quick Recap 

In Lab assignment 1 report we proposed to measure the performance by finding balance between cost and time.That is, we intend to do a cost vs time optimization. How do we do that? We plan to define a very simple metrics as shown in the table below :

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
      .saveAsTextFile(args(1))
```      
### Before we start...
As discussed in the feedback session for lab assignment-1, User Defined functions(UDF) kill the spark dataframe approach optimizations. Our [code](https://github.com/pradyot-09/ApacheSpark_intro/blob/master/lab2/GdeltAnalysis.md#code) consisted a UDF for discarding the unwanted numbers in 'AllNames' column. Hence, we removed the UDF as follows: 

```
val c = b.withColumn("AllNames",split(col("AllNames"),",")(0)) 
//removed the UDF
//val c = b.withColumn("AllNames", url_cleaner_udf(b("AllNames"))) //using user defined function to remove the numbers
```

### Naive Start :hatching_chick:

Before we run our spark application on the entire dataset, we wanted to filter down whether we should use our rdd or dataframe approach. Since it won't be viable to run each cluster configuration on both the implementation. We started with small number of instances of m4.large machines and then increased the instances gradually. We compared our RDD and DF implementation on m4.large machine.

##### RDD vs DF

| RDD vs DF Comparison | 
| -------------------- | 
| ![alt text][image1]  |

[image1]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/RDD%20vs%20DF/rddVsDfNew.PNG "RDD vs DF" 

The difference in execution time between RDD and Dataframe approach was 16 seconds for 864 segments which increased upto 56 seconds for 2974 segments. This difference seems trivial for now but when the application scales up to run all the segments of the dataset, this difference will become huge. Also as stated by us in lab assignment-1, we expected the Dataframe approach to be faster as it is compiled into execution plan and then executed. Therefore, spark can optimize the execution plan to reduce execution time.To still bolster our assumption we ran both our approaches on m4.large machines with 21 instances on whole dataset(4.1TB).

### Minimum Requirement
As mentioned in assignment manual the minimum requirement for assignment is to run the whole dataset on 20 instances of c4.8xlarge machines.
In this section we present the performance of our implementation on 20 instances of c4.8xlarge machines.

![Too much talk](https://github.com/pradyot-09/Big_Data_images/blob/master/images/trump_meme.gif)

##### Lets Start....
Since it was our first go, We slowly scaled the number of segments and machines. Then the two rookie authors of this blog thought they were ready to face the test and took the bold step of trying out the whole dataset on 20 c4.8xlarge core nodes.  Obviously we met our doom.

![Executor Memory Error](https://github.com/pradyot-09/Big_Data_images/blob/master/images/c4.8x%205%20errors/2019-10-12%20(1).png)

##### Bottleneck-1 : executor memory
The executors in the cluster were running out of memory. So we increased the executor and driver memory in the spark configuration. The new configuration were : ` --executor-memory 3g` and `--driver-memory 3g`. We increased the memory by 1gb starting from 1gb and it worked for 3gb :man_shrugging:.

| Performance          | 
| -------------------- | 
| ![alt text][image3]  |

[image3]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c4.8x%2020%20success/c4.8xlarge_naive.png "c4.8xlarge performance" 


Average core node CPU usage   |  Average cluster CPU usage 
:----------------------------:|:-----------------------------------:
![alt text][image4]           |  ![alt text][image5]

[image4]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c4.8x%2020%20success/performance.png "core node CPU usage"
[image5]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c4.8x%2020%20success/cluster_usage.png "cluster node CPU usage"

The task completed in 7 minutes approximately(including file writing task).But the Average core node CPU and cluster CPU usage were just above 30% :frowning_face:. After the naive run, we thought if we can improve the performance by tweaking the spark configuration :thought_balloon: ? 

### Let's dive into spark configuration
In the naive implementation we naively set executor memory to 3gb without any basis. In this section we explain how we got the best(according to us) performance from the cluster by tweaking spark configuration. The AWS [blog](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/) helped us in configuring spark.

We ran multiple experiments on whole dataset 5 with instances of C4.8xlarge machines to find the right amount of `spark.executor.cores` as shown table below :

| executor.cores | executor.memory         | time taken to complete the task (Approx.) | Average CPU Utilization (Approx.)|
| -------------- | ----------------------- | ----------------------------------------- | -------------------------------- | 
| 2              |         3g              | 23 minutes                                | 58%                              |
| 3              |         5g              | 25 minutes                                | 56%                              |
| 4              |         6g              | 21 minutes                                | 66%                              |
| 5              |         8g              | 24 minutes                                | 55%                              |
| 7              |         12g             | 28 minutes                                | 51%                              |

Clearly 4 executor cores suit the implementation. But how did we come up with executor memory (6GB)?

C4.8xlarge machine has **36vCores** and **60GB** memory. 

We first calculated the number of executors per instance:
```
Number of executors per instance = (total number of virtual cores per instance - 1)/ spark.executors.cores

Number of executors per instance = (36 - 1)/ 4 = 35 / 4 = 9 (rounded up)
//Subtracted one virtual core from the total number of virtual cores to reserve it for the Hadoop daemons.
```
Then, calculated RAM per instance
```
Total executor memory = total RAM per instance / number of executors per instance
Total executor memory = 59 / 9 = 6 (rounded down)
//Leave 1 GB for the Hadoop daemons.
```
Based on above calculations we set the executor memory to 6GB. 

The total executor memory includes the executor memory and overhead (spark.yarn.executor.memoryOverhead). 10 percent memory from the total executor memory is assigned to the memory overhead:
```
spark.yarn.executor.memoryOverhead = total executor memory * 0.10
spark.yarn.executor.memoryOverhead = 6 * 0.1 = 0.6 
```
Hence the executor.memoryOverhead is 600MB approximately. Also, we set the driver.memory equal to  executor.memory . We would be using this spark configuration in all future task runs.Now we have figured out the spark configuration and ready to test our fastest implementation :rocket: .

### Talk About Speed
In this section we show off the performance of our DF implemetation on 20 c4.8xlarge core nodes. We set the spark configuration as discussed in the above section (executor.cores=4 & executor.memory=6g). 

| Performance : c4.8xlarge (20 instances)| 
| -------------------------------------- | 
| ![alt text][image6]                    |

[image6]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c420%20faster/performance.png "c4.8xlarge performance" 

  Core node CPU usage         |  Cluster CPU usage 
:----------------------------:|:-----------------------------------:
![alt text][image7]           |  ![alt text][image8]


[image7]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c420%20faster/core_cpu.png "core node CPU usage"
[image8]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/c420%20faster/cluster_cpu.png "cluster node CPU usage"


Voila!! the new cluster configuration completed the task in 5.5 minutes. Also, the average CPU usage is approximately 60% which is almost double as compared to our previous naive run. But can we do even better in terms of CPU utilization? 

##### Bottleneck-2 : CPU utilization
After tweaking the spark configurations we were able to reach 60% CPU utilization on c4.8xlarge machines. 60% CPU utilization is good but not great. Can we reach 80%-90% CPU utilization? May be we can try the same spark configuration on different machines to find a cluster configuration which would be suitable for the problem and also ranks good based on our performance metrics.

### Comaprison
In this section we compare the performance of different cluster configuration to find the best match for the problem. For memory-intensive applications, R type instances are prefered over the other instance types. Hence we included R machines in our comparison. Obviously, it won't be viable to run the task on possible on all machines (TA's would kill us). We decided to run the task on machines similar in specification(vCores,RAM) to c4.8xlarge and also tried to alter the number of instances. We used the following combination of machines and instances :
 
 
| Machine_Type   | Cost per hour ($)       |  Number of Instances | time taken to complete the task (in minutes)| Cost of cluster |
| -------------- | ----------------------- | -------------------- | ------------------------------------------- | --------------- |
| c4.8xlarge     |         0.297           | 5                    |  21                                         |  0.519
| c4.8xlarge     |         0.297           | 10                   |  10.25                                      |  0.504
| c4.8xlarge     |         0.297           | 20                   |  5.55                                       |  0.546         
| c4.4xlarge     |         0.200           | 20                   |  8.55                                       |  0.568     
| m4.large       |         0.020           | 20                   |  78                                         |  0.520     
| r4.4xlarge     |         0.166           | 20                   |  8.9                                        |  0.491
| r4.8xlarge     |         0.319           | 20                   |  5.6                                        |  0.593

* NOTE: The machine rates used in this blog correspond to the price of spot instances as on 13/10/2019. *

Based on above table we can plot a graph to compare these clusters.


| Cost vs Time Tradeoff                  | 
| -------------------------------------- | 
| ![alt text][image9]                    |

[image9]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/costVsTime.PNG "Cost vs Time tradeoff"

### Results
Based on the comparison discussed in the comparison section the winner is r4.4xlarge with 20 instances followed by c4.8xlarge with 10 instances. Let's have look at the performance of r4.4xlarge machine :

| r4.4xlarge Performance | 
| ---------------------- | 
| ![alt text][image10]   |

[image10]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/r4.4x%2020/performance.png "r4.4xlarge performance" 

 Core node CPU usage          |  Cluster CPU usage 
:----------------------------:|:-------------------------:
![alt text][image11]          |  ![alt text][image12]


[image11]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/r4.4x%2020/core_node_cpu.png "core node CPU usage"
[image12]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/r4.4x%2020/cluster_cpu.png "cluster node CPU usage"

  Cluster Memory               |  Cluster load 
:-----------------------------:|:----------------------------:
 ![alt text][image13]          |   ![alt text][image14]


[image13]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/r4.4x%2020/cluster_memory.png "Cluster Memory"
[image14]: https://github.com/pradyot-09/Big_Data_images/blob/master/images/r4.4x%2020/satisfying_image.png "Cluster load"


Aren't those images satisfying :heart_eyes:? With r4.4xlarge machines we were able to reach **85%** average cluster CPU usage(approx.) and **90%** core node CPU usage(approx.). The task was completed in 8.9 minutes using 20 instances. According to our defined performance metrics this machine gives the best balance between cost and time. 

Although the performance metrics defined by us does not account for many factors such as effiecient memory usage, network usage ecectra. But it definitely gives us an approximation about the most suitable machine for a problem.


### Acknowledgement

We would like to express our sincere gratitude to the teaching assistants Robin Hes, Matthijs Brobbel, and Dorus Leliveld for providing guidance, comments and suggestions (including quick replies to *redundant* emails even on the weekends) throughout this lab without getting frustrated from our persistent and extremely naive questions. We would also like to thank them for providing access to the IAM accounts so that we could test our approaches flawlessly without getting into the hassle of contacting aws support again and again.


Group10 signing off! :wave:
#God_speed
