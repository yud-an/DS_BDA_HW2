package bigdata.hw2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Int;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.temporal.ChronoField.YEAR;

public class LogLevelCounter {
    /**
     * Function counts syslog priority levels per hour.
     * Parse log priority level and hour from string.
     * @param inputRDD - input rdd data for processing
     * @return result in JavaPairRDD<String, Integer>  format
     */
    public static JavaPairRDD<String, Integer> counLogLevelPerHour(JavaRDD<String> inputRDD){
        JavaPairRDD<String, Integer> rdd = inputRDD.mapToPair(w -> {
            String[] split = w.split("\\s");
            return new Tuple2<String, Integer>(split[2].substring(0,2) + "," + split[3] , 1);
        });
        JavaPairRDD<String, Integer> transformedRDD = rdd.reduceByKey((x,y) -> x + y).sortByKey();
        return transformedRDD;
    }
}
