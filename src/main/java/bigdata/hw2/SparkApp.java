package bigdata.hw2;

import java.util.Arrays;
import lombok.extern.log4j.Log4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Counts syslog priority levels per hour
 */
@Log4j
public class SparkApp {
    public static void main(String[] args) {
SparkSession s=SparkSession
      .builder()
      .master("local")
      .appName("SparkSQLApplication")
      .getOrCreate();
        Dataset<String> df= s.read().text(args[1]).as(Encoders.STRING());
log.info("============COUNTING...======================");
JavaRDD<Row> result=LogLevelEventCounter.countLogLevelPerHour(df);
log.info("============SVING TO" + args[1] + "======================");
            result.saveAsTextFile(args[1]);
       
    }
}
