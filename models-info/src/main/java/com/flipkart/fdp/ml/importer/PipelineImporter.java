package com.flipkart.fdp.ml.importer;

import com.flipkart.fdp.ml.modelinfo.ModelInfo;
import com.flipkart.fdp.ml.modelinfo.PipelineModelInfo;
import com.flipkart.fdp.ml.transformer.PipelineModelTransformer;

/**
 * Created by akshay.us on 8/18/16.
 */
public class PipelineImporter {
    public static PipelineModelTransformer importAndGetTransformer(byte[] serializedModelInfo) {
        ModelInfo modelInfo = ModelImporter.importModelInfo(serializedModelInfo);
        if(! (modelInfo instanceof PipelineModelInfo)) {
            throw new RuntimeException("Model to be imported is not a pipeline model. It is "+ modelInfo.getClass());
        }
        return (PipelineModelTransformer) modelInfo.getTransformer();
    }
}
