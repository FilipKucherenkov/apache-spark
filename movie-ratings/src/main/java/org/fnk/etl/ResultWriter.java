package org.fnk.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.fnk.config.JobConfig;

public class ResultWriter {

    public static void write(Dataset<Row> result, JobConfig config) {
        result.write()
                .format("avro")
                .mode("overwrite")
                .save(config.getOutputPath());
    }
}
