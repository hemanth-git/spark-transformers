package com.flipkart.fdp.ml.adapter;

import com.flipkart.fdp.ml.ModelInfoAdapterFactory;
import com.flipkart.fdp.ml.modelinfo.AbstractModelInfo;
import com.flipkart.fdp.ml.modelinfo.PipelineModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Transformer;
import org.apache.spark.sql.DataFrame;

/**
 * Transforms Spark's {@link org.apache.spark.ml.PipelineModel} to  {@link PipelineModel} object
 * that can be exported through {@link com.flipkart.fdp.ml.export.ModelExporter}
 */
@Slf4j
public class PipelineModelInfoAdapter extends AbstractModelInfoAdapter<org.apache.spark.ml.PipelineModel, PipelineModel> {
    @Override
    public PipelineModel getModelInfo(final org.apache.spark.ml.PipelineModel from, final DataFrame df) {
        final AbstractModelInfo stages[] = new AbstractModelInfo[from.stages().length];
        for (int i = 0; i < from.stages().length; i++) {
            Transformer sparkModel = from.stages()[i];
            stages[i] = (AbstractModelInfo) ModelInfoAdapterFactory.getAdapter(sparkModel.getClass()).adapt(sparkModel, df);
        }
        final PipelineModel modelInfo = new PipelineModel(stages);
        return modelInfo;
    }

    @Override
    public Class<org.apache.spark.ml.PipelineModel> getSource() {
        return org.apache.spark.ml.PipelineModel.class;
    }

    @Override
    public Class<PipelineModel> getTarget() {
        return PipelineModel.class;
    }
}
