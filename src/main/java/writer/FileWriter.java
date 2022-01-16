package writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import model.ComplementAlimentaire;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class FileWriter implements Consumer<Dataset<Row>> {

    private final String outputPathStr;

    @Override
    public void accept(Dataset<Row> element) {

        try {
            element.toDF().coalesce(2).write().mode(SaveMode.Overwrite).option("delimiter", ";").option("header", true).csv(outputPathStr);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

