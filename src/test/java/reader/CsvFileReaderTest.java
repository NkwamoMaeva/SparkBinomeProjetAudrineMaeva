package reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mapper.ConvertFileReader;
import model.ComplementAlimentaire;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvFileReaderTest {
    @Test
    public void ShouldReadFile() throws IOException {
        Config config = ConfigFactory.load();
        String inputPathTest = config.getString("app.data.inputTest");

        CsvFileReader reader = new CsvFileReader(inputPathTest);
        ConvertFileReader convertFileReader = new ConvertFileReader();

        Dataset<Row> rows = reader.get();
        Dataset<ComplementAlimentaire> CA = convertFileReader.apply(rows);

//        rows = rows.select("idcomplement");
        int expected = 4;
        assertThat(
                rows.count()
        ).isGreaterThan(0L);
        assertThat(CA.count()).isEqualTo(expected);
    }

}