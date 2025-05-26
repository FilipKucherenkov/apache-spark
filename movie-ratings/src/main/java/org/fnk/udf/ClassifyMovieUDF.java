package org.fnk.udf;

import org.apache.spark.sql.api.java.UDF1;

public class ClassifyMovieUDF implements UDF1<Double, String> {
    @Override
    public String call(Double rating) throws Exception {
        if(rating >= 4.0){
            return "Excellent";
        }else if(rating >= 2.0){
            return "Good";
        }else{
            return "Bad";
        }
    }
}
