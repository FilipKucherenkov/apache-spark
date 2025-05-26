package org.fnk.etl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.fnk.config.JobConfig;

public class DataLoader {


    public static Dataset<Row> load(SparkSession spark, JobConfig config, DatasetItem datasetItem) {
        return spark.read()
                .option("delimiter", datasetItem.delimiter)
                .option("header", false)
                .schema(getSchema(datasetItem))
                .csv(config.getInputPath() + "/" + datasetItem.fileName);
    }


    private static StructType getSchema(DatasetItem datasetItem){
        return switch (datasetItem){
            case RATINGS -> new StructType()
                    .add("userId", DataTypes.IntegerType)
                    .add("movieId", DataTypes.IntegerType)
                    .add("rating", DataTypes.IntegerType)
                    .add("timestamp", DataTypes.LongType);

            case MOVIES ->new StructType()
                    .add("movieId", DataTypes.IntegerType)
                    .add("title", DataTypes.StringType)
                    .add("releaseDate", DataTypes.StringType)
                    .add("videoReleaseDate", DataTypes.StringType)
                    .add("imdbUrl", DataTypes.StringType)
                    .add("unknown", DataTypes.IntegerType)
                    .add("action", DataTypes.IntegerType)
                    .add("adventure", DataTypes.IntegerType)
                    .add("animation", DataTypes.IntegerType)
                    .add("childrens", DataTypes.IntegerType)
                    .add("comedy", DataTypes.IntegerType)
                    .add("crime", DataTypes.IntegerType)
                    .add("documentary", DataTypes.IntegerType)
                    .add("drama", DataTypes.IntegerType)
                    .add("fantasy", DataTypes.IntegerType)
                    .add("filmNoir", DataTypes.IntegerType)
                    .add("horror", DataTypes.IntegerType)
                    .add("musical", DataTypes.IntegerType)
                    .add("mystery", DataTypes.IntegerType)
                    .add("romance", DataTypes.IntegerType)
                    .add("sciFi", DataTypes.IntegerType)
                    .add("thriller", DataTypes.IntegerType)
                    .add("war", DataTypes.IntegerType)
                    .add("western", DataTypes.IntegerType);
        };
    }
}
