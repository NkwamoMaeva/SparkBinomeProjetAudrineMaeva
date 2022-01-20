import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class RevisionDatasetTest {
    @Test
    public Dataset<Float> review(){

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkApp");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        List<Float> floatList = Arrays.asList((float)1, (float)2, (float)3, (float)4, (float)5);
        Dataset<Float> input = sparkSession.createDataset(floatList, Encoders.FLOAT());
        return input;
    }
}
