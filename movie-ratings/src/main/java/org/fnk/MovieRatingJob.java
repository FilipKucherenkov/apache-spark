package org.fnk;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.fnk.config.JobConfig;
import org.fnk.config.SparkSessionFactory;
import org.fnk.etl.DataLoader;
import org.fnk.etl.DatasetItem;
import org.fnk.etl.MovieRatingTransformer;
import org.fnk.etl.ResultWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.spark.sql.functions.*;


public class MovieRatingJob {

    private static final Logger logger = LoggerFactory.getLogger(MovieRatingJob.class);

    public static void main(String[] args) {
        try {
            JobConfig config = new JobConfig("application.properties");
            MovieRatingTransformer movieRatingTransformer = new MovieRatingTransformer();
            SparkSession spark = SparkSessionFactory.create(config);


            Dataset<Row> moviesDataset = DataLoader.load(spark, config, DatasetItem.MOVIES);
            Dataset<Row> ratingsDataset = DataLoader.load(spark, config, DatasetItem.RATINGS);

            Dataset<Row> joinedData = ratingsDataset.join(broadcast(moviesDataset), "movieId");

            Dataset<Row> result = movieRatingTransformer.transform(joinedData);

            ResultWriter.write(result, config);


            spark.stop();
        } catch (Exception e) {
            logger.error("Spark job failed", e);
            System.exit(1);
        }

    }
}
