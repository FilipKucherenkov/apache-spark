package org.fnk.config;

import java.io.InputStream;
import java.util.Properties;

public class JobConfig {
    private final Properties props = new Properties();

    public JobConfig(String fileName) throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(fileName)) {
            props.load(input);
        }
    }

    public String getInputPath() {
        return props.getProperty("input.path");
    }

    public String getOutputPath() {
        return props.getProperty("output.path");
    }

    public String getAppName() {
        return props.getProperty("spark.app.name", "Movie Ratings Analyser");
    }

    public String getMaster() {
        return props.getProperty("spark.master", "local[*]");
    }
}
