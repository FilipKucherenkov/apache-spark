package org.fnk.config;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.fnk.udf.ClassifyMovieUDF;

public class SparkSessionFactory {

    public static SparkSession create(JobConfig config) {
        SparkSession spark = SparkSession.builder()
                .appName(config.getAppName())
                .master(config.getMaster())
                .getOrCreate();

        spark.udf().register("classifyMovie", new ClassifyMovieUDF(), DataTypes.StringType);
        return spark;
    }
}
