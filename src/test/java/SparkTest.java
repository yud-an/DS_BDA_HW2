import bigdata.hw2.LogLevelCounter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparkTest {

    private JavaSparkContext sparkContext;
    private final List<String> input = new ArrayList<>();
    private final List<Tuple2<String, Integer>> expectedOutput = new ArrayList<>();

    @BeforeEach
    public void init() throws ParseException {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("junit");
        sparkContext = new JavaSparkContext(conf);

        input.add("Nov 2 12:22:22 3");
        input.add("Nov 2 12:22:22 4");
        input.add("Nov 2 12:22:22 3");

        expectedOutput.add(new Tuple2<String, Integer>("12,3", 2));
        expectedOutput.add(new Tuple2<String, Integer>("12,4", 1));
    }

    @Test
    public void testRDD() {
        JavaRDD<String> inputRDD = sparkContext.parallelize(input);
        JavaPairRDD<String, Integer> resultRDD = LogLevelCounter.counLogLevelPerHour(inputRDD);
        List<Tuple2<String, Integer>> resultList = resultRDD.collect();
        assertEquals(expectedOutput.size(), resultList.size());
        assertTrue(resultList.containsAll(expectedOutput));
    }
}
