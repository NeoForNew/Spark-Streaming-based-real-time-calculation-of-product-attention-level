package com.shiyanlou.simulator;

import java.io.Serializable;
import java.util.List;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;
import org.apache.spark.api.java.Optional;
//import com.google.common.base.Optional;

public class StreamingGoods implements Serializable{
    private static final long serialVersionUID = 1L;
    // Define a folder to save the data of the previous RDD. This folder is created automatically and does not need to be created in advance
    private static String checkpointDir = "checkDir";
    public static void main(String[] args) throws InterruptedException{
        SparkConf sparkConf = new SparkConf().setAppName("StreamingGoods").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf,new Duration(5000));
        jsc.checkpoint(checkpointDir);
        JavaReceiverInputDStream<String> jds = jsc.socketTextStream("127.0.0.1", 9999);
        JavaDStream<String> mess = jds.map(new Function<String,String>(){
            private static final long serialVersionUID = 1L;
            public String call(String arg0) throws Exception {
                // TODO Auto-generated method stub
                return arg0;
            }});
        mess.print();
        JavaPairDStream<String, Double> splitMess = jds.mapToPair(new PairFunction<String,String,Double>(){
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Double> call(String line) throws Exception {
                // TODO Auto-generated method stub.
                String[] lineSplit = line.toString().split("::");
                Double followValue = Double.parseDouble(lineSplit[1])*0.8+Double.parseDouble(lineSplit[2])*0.6+Double.parseDouble(lineSplit[3])*1+Double.parseDouble(lineSplit[4])*1;
                return new Tuple2<String, Double>(lineSplit[0], followValue);
            }});
        JavaPairDStream<String, Double> UpdateFollowValue = splitMess.updateStateByKey(new Function2<List<Double>,Optional<Double>,Optional<Double>>(){

            public Optional<Double> call(List<Double> newValues,
                                         Optional<Double> statValue) throws Exception {
                // TODO Auto-generated method stub
                Double updateValue = statValue.or(0.0);
                for (Double values : newValues) {
                    updateValue += values;
                }
                return Optional.of(updateValue);
            }},new HashPartitioner(jsc.sparkContext().defaultParallelism()));
        UpdateFollowValue.foreachRDD(new VoidFunction<JavaPairRDD<String,Double>>(){
            private static final long serialVersionUID = 1L;
            public void call(JavaPairRDD<String, Double> followValue) throws Exception {
                // TODO Auto-generated method stub
                JavaPairRDD<Double,String> followValueSort = followValue.mapToPair(new PairFunction<Tuple2<String,Double>,Double,String>(){

                    public Tuple2<Double, String> call(
                            Tuple2<String, Double> valueToKey) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<Double,String>(valueToKey._2,valueToKey._1);
                    }
                }).sortByKey(false);
                List<Tuple2<String,Double>> list = followValueSort.mapToPair(new PairFunction<Tuple2<Double,String>,String, Double>() {

                    public Tuple2<String, Double> call(
                            Tuple2<Double, String> arg0) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2<String,Double>(arg0._2,arg0._1);
                    }
                }).take(10);



                try {
                    FileWriter myWriter = new FileWriter("record.txt", true);
                    for (Tuple2<String,Double> tu : list) {
                        myWriter.write("Product ID: "+tu._1+"  Attention rate: "+tu._2+"\n");
                        System.out.println("Product ID: "+tu._1+"  Attention rate: "+tu._2);
                    }

                    myWriter.close();
                    System.out.println("Successfully wrote to the file.");
                } catch (IOException e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }

            }});

        jsc.start();
        jsc.awaitTermination();
    }
}