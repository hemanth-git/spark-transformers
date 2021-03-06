package com.flipkart.fdp.ml.adapter;

import com.flipkart.fdp.ml.modelinfo.DecisionTreeModelInfo;
import com.flipkart.fdp.ml.modelinfo.RandomForestModelInfo;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Transforms Spark's {@link RandomForestModel} in MlLib to  {@link RandomForestModelInfo} object
 * that can be exported through {@link com.flipkart.fdp.ml.export.ModelExporter}
 */
public class RandomForestModelInfoAdapter
        extends AbstractModelInfoAdapter<RandomForestModel, RandomForestModelInfo> {

    private final DecisionTreeModelInfoAdapter bridge = new DecisionTreeModelInfoAdapter();

    private RandomForestModelInfo visitForest(final RandomForestModel randomForestModel) {
        final RandomForestModelInfo randomForestModelInfo = new RandomForestModelInfo();

        Set<String> inputKeys = new LinkedHashSet<String>();
        inputKeys.add("features");
        randomForestModelInfo.setInputKeys(inputKeys);

        Set<String> outputKeys = new LinkedHashSet<String>();
        outputKeys.add("prediction");
        randomForestModelInfo.setOutputKeys(outputKeys);

        randomForestModelInfo.setAlgorithm(randomForestModel.algo().toString());

        final DecisionTreeModel[] decisionTreeModels = randomForestModel.trees();
        for (DecisionTreeModel i : decisionTreeModels) {
            DecisionTreeModelInfo tree = bridge.getModelInfo(i);
            randomForestModelInfo.getTrees().add(tree);
        }
        return randomForestModelInfo;
    }

    @Override
    public RandomForestModelInfo getModelInfo(RandomForestModel from) {
        return visitForest(from);
    }

    @Override
    public Class<RandomForestModel> getSource() {
        return RandomForestModel.class;
    }

    @Override
    public Class<RandomForestModelInfo> getTarget() {
        return RandomForestModelInfo.class;
    }
}
