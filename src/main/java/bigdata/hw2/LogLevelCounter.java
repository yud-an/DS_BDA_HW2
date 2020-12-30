package bigdata.hw2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Int;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static java.time.temporal.ChronoField.YEAR;

private static DateTimeFormatter formatter=new DateTimeFormatterBuilder()
.appendPattern("MMM dd HH:mm:ss")
.parseDefaulting(Year,value 2018)
.toFormatter();

public class LogLevelEventCounter {
    /**
     * Function counts syslog priority levels per hour.
     * Parse log priority level and hour from string.
     * @param inputRDD - input rdd data for processing
     * @return result in JavaPairRDD<String, Integer>  format
     */
    public static JavaRDD<Row> counLogLevelPerHour(Dataset<String> inputDataset){
        Dataset<String> wor = inputDataset.map(w -> Arrays.toString(w.split(regex "\\s")).Encoders.STRING());
Dataset<LogLevelHour> logLevelHourDataset=wor.map(w -> {String[] logFields=w.split(",");
LocalDateTime date=LocalDateTime.parse(logFields[2],formatter);
            return new LogLevelHour(logFields[1],date.getHour());}, Encoders.bean(LogLevelHour.class)).coalesce(1);
         
        Dataset<Row> t = logLevelHourDataset.groupBy();
log.info("================Result===============");
t.show();
        return t.toJavaRDD;
    }
}
