package edu.miu.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
        // local = "DATA/input/dataFile.txt"
        JavaRDD<String> lines = sc.textFile(args[0]);

        // Calculate word count
//        JavaPairRDD<String, Integer> counts = lines
//                .flatMapToPair(line ->
//                    Arrays.asList(line.split(" ")).stream().map(s -> new Tuple2<>(s, 1)).collect(Collectors.toList())
//                )
//                .reduceByKey((x, y) -> x + y);
        //TODO this is for local testing only - run in spark-submit : need to remove this line
        //  local = "DATA/output/count1"
        //  FileUtils.deleteDirectory(new File(args[2]));
//        counts.saveAsTextFile(args[1]);

        sc.close();
    }
}
