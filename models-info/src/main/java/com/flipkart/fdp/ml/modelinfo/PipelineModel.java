package com.flipkart.fdp.ml.modelinfo;

import com.flipkart.fdp.ml.transformer.Transformer;
import lombok.Getter;

import java.util.Map;

/**
 * Represents information for a pipeline model.
 * Also implements transformer interface representing its ability to transform an input
 */
@Getter
public class PipelineModel extends AbstractModelInfo implements Transformer {

    private final AbstractModelInfo stages[];
    private final Transformer transformers[];


    public PipelineModel(AbstractModelInfo[] stages) {
        super();
        this.stages = stages;
        this.setInputKeys(stages[0].getInputKeys());
        this.setOutputKey(stages[stages.length-1].getOutputKey());
        transformers = new Transformer[stages.length];
        for (int i = 0; i < transformers.length; i++) {
            transformers[i] = stages[i].getTransformer();
        }
    }

    @Override
    public Transformer getTransformer() {
        return this;
    }

    @Override
    public void transform(final Map<String, Object> input) {
        for (Transformer transformer : transformers) {
            transformer.transform(input);
        }
    }
}
