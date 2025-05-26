package org.fnk.etl;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class MovieRatingTransformer {

    public Dataset<Row> transform(Dataset<Row> dataset){
        List<String> genres = List.of(
                "unknown", "action", "adventure", "animation", "childrens", "comedy", "crime",
                "documentary", "drama", "fantasy", "filmNoir", "horror", "musical", "mystery",
                "romance", "sciFi", "thriller", "war", "western"
        );

        Column[] genreColumns = genres.stream()
                .map(g -> when(col(g).equalTo(1), lit(g)).otherwise(null))
                .toArray(Column[]::new);


        Dataset<Row> tmp = dataset
                .withColumn("genres", array(genreColumns))
                .withColumn("genres", expr("filter(genres, x -> x is not null)"))
                .groupBy(col("movieId"), col("title"), col("genres"))
                .agg(
                        avg("rating").alias("movie_rating"),
                        count("rating").alias("number_of_reviews")
                )
                .filter(col("number_of_reviews").geq(lit(20)))
                .orderBy(col("movie_rating").desc());

        Dataset<Row> result = tmp.withColumn("movie_rating", callUDF("classifyMovie", col("movie_rating")));

        result.show(1000, false);
        return result;
    }
}
