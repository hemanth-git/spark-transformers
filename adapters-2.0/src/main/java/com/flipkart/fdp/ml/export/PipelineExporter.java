package com.flipkart.fdp.ml.export;

import org.apache.spark.ml.PipelineModel;

/**
 * Created by akshay.us on 8/18/16.
 */
public class PipelineExporter {
    public static byte[] export(PipelineModel pipelineModel) {
        return ModelExporter.export(pipelineModel);
    }
}
