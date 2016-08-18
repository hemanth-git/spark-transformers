package com.flipkart.fdp.ml.export;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;

/**
 * Created by akshay.us on 8/18/16.
 */
public class PipelineExporter {
    public static byte[] export(PipelineModel pipelineModel, DataFrame df) {
        return ModelExporter.export(pipelineModel, df);
    }
}
