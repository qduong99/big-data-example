package edu.miu.spark;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
        JavaRDD<String> lines = sc.textFile("DATA/input/dataFile.txt");

        // Calculate word count
        JavaPairRDD<String, Integer> counts = lines
                .flatMapToPair(line ->
                    Arrays.asList(line.split(" ")).stream().map(s -> new Tuple2<>(s, 1)).collect(Collectors.toList())
                )
                .reduceByKey((x, y) -> x + y);
        //TODO this is for local testing only - run in spark-submit : need to remove this line
        FileUtils.deleteDirectory(new File("DATA/output/count1"));
        counts.saveAsTextFile("DATA/output/count1");

        int threshold = 2;
        JavaPairRDD<String, Integer> counts2 = counts.filter(v1 -> {
            if (v1._2() > threshold) {
                return true;
            }
            return false;
        });

        JavaPairRDD<String, Integer> countChars = counts2
                .map(tuple -> {
                    List<Tuple2> result = new ArrayList<>();
                    for (String s : tuple._1().split("")) {
                        result.add(new Tuple2<>(s, tuple._2));
                    }
                    return result;
                })
                .flatMap(w -> Arrays.asList(w.toArray()))
                .mapToPair(m -> {
                    Tuple2<String, Integer> t = (Tuple2<String, Integer>) m;
                    return new Tuple2<>(t._1, t._2);
                })
                .reduceByKey((x, y) -> x + y);


        // Save the word count back out to a text file, causing evaluation
        //TODO this is for local testing only - run in spark-submit : need to remove this line
        FileUtils.deleteDirectory(new File("DATA/output/count2"));
        countChars.saveAsTextFile("DATA/output/count2");

        sc.close();
    }
}
