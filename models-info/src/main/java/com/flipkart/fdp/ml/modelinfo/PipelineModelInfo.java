package com.flipkart.fdp.ml.modelinfo;

import com.flipkart.fdp.ml.transformer.PipelineModelTransformer;
import com.flipkart.fdp.ml.transformer.Transformer;
import lombok.Getter;

/**
 * Represents information for a pipeline model
 */
@Getter
public class PipelineModelInfo extends AbstractModelInfo {

    private AbstractModelInfo stages[];

    public PipelineModelInfo(AbstractModelInfo[] stages) {
        super();
        this.stages = stages;
        this.setInputKeys(stages[0].getInputKeys());
        this.setOutputKey(stages[stages.length-1].getOutputKey());
    }

    /**
     * @return an corresponding {@link PipelineModelTransformer} for this model info
     */
    @Override
    public Transformer getTransformer() {
        return new PipelineModelTransformer(this);
    }
}
