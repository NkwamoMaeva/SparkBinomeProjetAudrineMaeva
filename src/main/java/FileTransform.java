import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import mapper.ConvertFileReader;
import model.ComplementAlimentaire;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import reader.CsvFileReader;
import writer.FileWriter;

import static org.apache.spark.sql.functions.count;

@Slf4j
public class FileTransform {
    public static void main( String[] args ) {
        Config config = ConfigFactory.load();
        String inputPath = config.getString("app.data.input");
        String outputPath = config.getString("app.data.output");

        System.out.println( "Hello Alimentation Complement Collections!" );

        CsvFileReader reader = new CsvFileReader(inputPath);

        ConvertFileReader convertFileReader = new ConvertFileReader();

        Dataset<Row> rows = reader.get();
        Dataset<ComplementAlimentaire> complementAlimentaire = convertFileReader.apply(rows);

        Dataset<Row> statds2 = complementAlimentaire.groupBy("famillesPlantes").agg(count("idComplement").as("Nbre par Famille de plante"));
        statds2.show();

        FileWriter writer = new FileWriter(outputPath);
        writer.accept(statds2);
    }
}
