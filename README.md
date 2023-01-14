# Spark-Streaming-based-real-time-calculation-of-product-attention-level
The target of this project is to analyze which products have received the most attention during this period based on how many times a product has been clicked, how long the user has stayed, and whether the user has favored the product.

## Authors
Zhenghong Xiao xzhe@kth.se

Ayman Osmam Abubaker aymanoa@kth.se 

Disen Ling disen@kth.se 
## Project Description

An e-commerce company can analyze which products have received the most atten- tion during this period based on how many times a product has been clicked, how long the user has stayed, and whether the user has favorited the product. It is also possible to recommend products to users based on this data. Spark Streaming will be used to build a real-time data processing system to calculate which products are currently the most popular on the e-commerce platform. And finally the data will be stored in a HDFS.
## Tools
Streaming processing models will be applied in this project. Therefore, the following tools will be used in this project:
1. File system: Hadoop Distributed File System(HDFS)
2. Streaming processing tools:Spark Streaming
3. Compilation environment: Java
## Program
### Generate Streaming Data

This function is achieved by the program called (SimulatorSocket.java)[https://github.com/NeoForNew/Spark-Streaming-based-real-time-calculation-of-product-attention-level/blob/main/src/com/shiyanlou/simulator/SimulatorSocket.java]. Some assumptions are made about the simulated goods data and for a single message:

• There are 200 items in total.

• The number of times the user views a given item is at most 5.

• The time spent by the user viewing a given item is at most 10.

• If the user favorites an item, we assign +1 and if they give a poor rating we assign -1. Otherwise 0.

• The user is less likely to purchase any given item. 

The message format will look like this:
Product ID::Views::Length of stay::Whether to collect::Number of pieces purchased The following are some examples of raw data:

goodsID-68::1::2.3782935::1::0

goodsID-150::1::1.1446879::1::1

goodsID-143::1::3.3518002::0::0

goodsID-179::2::9.766784::0::0

goodsID-191::2::8.431307::0::0

goodsID-132::4::9.423632::0::2

### Connect to Spark Streaming

This function is achieved by the prgram called(StreamingGood.java)[https://github.com/NeoForNew/Spark-Streaming-based-real-time-calculation-of-product-attention-level/blob/main/src/com/shiyanlou/simulator/StreamingGoods.java]. By giving each value in the raw data a weight, we can calculate the attention level of products. Some examples are shown below:

Product ID: goodsID-47 Attention rate: 14.48421162

Product ID: goodsID-19 Attention rate: 9.2231634

Product ID: goodsID-130 Attention rate: 8.758230619999999

Product ID: goodsID-48 Attention rate: 8.387603559999999 ...

### Store data into Hadoop
The function of storing calculated data in Hadoop is achieved by the program [FileWrite](https://github.com/NeoForNew/Spark-Streaming-based-real-time-calculation-of-product-attention-level/blob/main/src/com/shiyanlou/simulator/FileWrite.java) which simply writes calculated results to Hadoop file system.

![image](https://github.com/NeoForNew/Spark-Streaming-based-real-time-calculation-of-product-attention-level/blob/main/pic/result.jpg)
