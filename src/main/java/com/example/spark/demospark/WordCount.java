package com.example.spark.demospark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class WordCount {
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) {

		LOGGER.info("marker");

		final Pattern SPACE = Pattern.compile(" ");

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
		.setMaster("local")
		;

		String file = "src/main/resources/spark_example.txt";
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(file, 1);

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
		JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			LOGGER.info("word {} count {}", tuple._1(), tuple._2());
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
		ctx.close();
	}

}
