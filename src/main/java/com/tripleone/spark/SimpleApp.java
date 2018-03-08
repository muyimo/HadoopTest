package com.tripleone.spark;

/**
 * Created by fwj on 17/12/20.
 * spark的测试app
 */


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "/usr/local/spark/README.md";
        SparkSession sparkSession = SparkSession.builder()
                .appName("Simple Application").getOrCreate();
        Dataset<String> logData = sparkSession.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a:" + numAs
                + ",Lines with b: " + numBs);
        sparkSession.stop();
    }
}
