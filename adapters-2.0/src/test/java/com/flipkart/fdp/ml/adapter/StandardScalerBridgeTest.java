package com.flipkart.fdp.ml.adapter;

import com.flipkart.fdp.ml.export.ModelExporter;
import com.flipkart.fdp.ml.importer.ModelImporter;
import com.flipkart.fdp.ml.transformer.Transformer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by akshay.us on 3/14/16.
 */
public class StandardScalerBridgeTest extends SparkTestBase {

    private final double data[][] = {{-2.0, 2.3, 0.0},
            {0.0, -5.1, 1.0},
            {1.7, -0.6, 3.3}};

    private final double resWithMean[][] = {{-1.9, 3.433333333333, -1.433333333333},
            {0.1, -3.966666666667, -0.433333333333},
            {1.8, 0.533333333333, 1.866666666667}};

    private final double resWithStd[][] = {{-1.079898494312, 0.616834091415, 0.0},
            {0.0, -1.367762550529, 0.590968109266},
            {0.917913720165, -0.160913241239, 1.950194760579}};

    private final double resWithBoth[][] = {{-1.0259035695965, 0.920781324866, -0.8470542899497},
            {0.0539949247156, -1.063815317078, -0.256086180682},
            {0.9719086448809, 0.143033992212, 1.103140470631}};

    @Test
    public void testStandardScaler() {


        JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(data[0])),
                RowFactory.create(2.0, Vectors.dense(data[1])),
                RowFactory.create(3.0, Vectors.dense(data[2]))
        ));

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(jrdd, schema);

        //train model in spark
        StandardScalerModel sparkModelNone = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledOutput")
                .setWithMean(false)
                .setWithStd(false)
                .fit(df);

        StandardScalerModel sparkModelWithMean = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledOutput")
                .setWithMean(true)
                .setWithStd(false)
                .fit(df);

        StandardScalerModel sparkModelWithStd = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledOutput")
                .setWithMean(false)
                .setWithStd(true)
                .fit(df);

        StandardScalerModel sparkModelWithBoth = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledOutput")
                .setWithMean(true)
                .setWithStd(true)
                .fit(df);


        //Export model, import it back and get transformer
        byte[] exportedModel = ModelExporter.export(sparkModelNone);
        final Transformer transformerNone = ModelImporter.importAndGetTransformer(exportedModel);

        exportedModel = ModelExporter.export(sparkModelWithMean);
        final Transformer transformerWithMean = ModelImporter.importAndGetTransformer(exportedModel);

        exportedModel = ModelExporter.export(sparkModelWithStd);
        final Transformer transformerWithStd = ModelImporter.importAndGetTransformer(exportedModel);

        exportedModel = ModelExporter.export(sparkModelWithBoth);
        final Transformer transformerWithBoth = ModelImporter.importAndGetTransformer(exportedModel);


        //compare predictions
        List<Row> sparkNoneOutput = sparkModelNone.transform(df).orderBy("label").select("features", "scaledOutput").collectAsList();
        assertCorrectness(sparkNoneOutput, data, transformerNone);

        List<Row> sparkWithMeanOutput = sparkModelWithMean.transform(df).orderBy("label").select("features", "scaledOutput").collectAsList();
        assertCorrectness(sparkWithMeanOutput, resWithMean, transformerWithMean);

        List<Row> sparkWithStdOutput = sparkModelWithStd.transform(df).orderBy("label").select("features", "scaledOutput").collectAsList();
        assertCorrectness(sparkWithStdOutput, resWithStd, transformerWithStd);

        List<Row> sparkWithBothOutput = sparkModelWithBoth.transform(df).orderBy("label").select("features", "scaledOutput").collectAsList();
        assertCorrectness(sparkWithBothOutput, resWithBoth, transformerWithBoth);

    }

    private void assertCorrectness(List<Row> sparkOutput, double[][] expected, Transformer transformer) {
        for (int i = 0; i < 2; i++) {
            double[] input = ((Vector) sparkOutput.get(i).get(0)).toArray();

            Map<String, Object> data = new HashMap<String, Object>();
            data.put("features", input);
            transformer.transform(data);
            double[] transformedOp = (double[]) data.get("scaledOutput");

            double[] sparkOp = ((Vector) sparkOutput.get(i).get(1)).toArray();
            assertArrayEquals(transformedOp, sparkOp, 0.01);
            assertArrayEquals(transformedOp, expected[i], 0.01);
        }
    }
}
