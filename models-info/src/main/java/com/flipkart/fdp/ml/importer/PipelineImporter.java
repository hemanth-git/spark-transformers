package com.flipkart.fdp.ml.importer;

import com.flipkart.fdp.ml.modelinfo.ModelInfo;
import com.flipkart.fdp.ml.modelinfo.PipelineModel;

/**
 * Created by akshay.us on 8/18/16.
 */
public class PipelineImporter {
    public static PipelineModel importAndGetTransformer(byte[] serializedModelInfo) {
        ModelInfo modelInfo = ModelImporter.importModelInfo(serializedModelInfo);
        if(! (modelInfo instanceof PipelineModel)) {
            throw new RuntimeException("Model to be imported is not a pipeline model. It is "+ modelInfo.getClass());
        }
        return (PipelineModel) modelInfo.getTransformer();
    }
}
