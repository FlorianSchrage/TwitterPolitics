/**
 * 
 */
package TwitterPolitics.Project.batchProcessing;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.json.JSONObject;
import org.json4s.JsonAST.JValue;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.immutable.List;

/**
 * @author Steffen Terheiden
 *
 */
public class LDATopicCreation {

	public static void main(String[] args) {
		System.out.println("LDATopicCreation running...");
		TestMongo.getSparkContext(TestMongo.Collections.TWEETS).setLogLevel("ERROR");
		SQLContext sqlContext = new SQLContext(TestMongo.getSparkContext(TestMongo.Collections.TWEETS));
		JavaRDD<Row> jrdd = TestMongo.getRDDs(TestMongo.Collections.TWEETS).map(d -> {
			// System.out.println("Processing: " + d.toJson());
			try {
				return RowFactory.create(0, new JSONObject(new JSONObject(d.toJson()).getString(TestMongo.RECORD)).getString("text"));
			} catch (JSONException e) {
				System.out.println(e);
				System.out.println("No JSON: " + d.toJson());
				return RowFactory.create(0, d.toString());
			}

		});

		StructType schema = new StructType(new StructField[] {
				new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
				new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
		});

		DataFrame sentenceDataFrame = sqlContext.createDataFrame(jrdd, schema).drop("label");
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		// TODO: solve problems with links
		// RegexTokenizer regexTokenizer = new RegexTokenizer()
		// .setInputCol("sentence")
		// .setOutputCol("words")
		// .setPattern("\\W");
		DataFrame wordsDataFrame = tokenizer.transform(sentenceDataFrame).drop("sentence");
		wordsDataFrame.show();

		CountVectorizerModel cvModel = new CountVectorizer()
				.setInputCol("words")
				.setOutputCol("features")
				.setMinDF(2)
				.fit(wordsDataFrame);
		Arrays.stream(cvModel.vocabulary()).forEach(s -> System.out.println("Param Ma: " + s));
		DataFrame featureDataFrame = cvModel.transform(wordsDataFrame);
		featureDataFrame.show();

		// Trains a LDA model
		LDA lda = new LDA()
				.setK(10)
				.setMaxIter(10);
		LDAModel model = lda.fit(featureDataFrame);

		// performance evaluation
		// System.out.println("logLikelihood: " + model.logLikelihood(featureDataFrame));
		// System.out.println("logPerplexity: " + model.logPerplexity(featureDataFrame));

		// Shows the result
		DataFrame topics = model.describeTopics(5);
		Column termIndices = topics.col("termIndices");
		System.out.println(termIndices.getItem("termIndices"));

		Seq<LogicalPlan> children = topics.logicalPlan().children();
		List<Tuple2<String, JValue>> jsonFields = children.apply(0).jsonFields();

		System.out.println(jsonFields);
		topics.show(false);
		model.transform(featureDataFrame).show(false);

		// LDATopicCreation processor = new LDATopicCreation();
		// processor.createTopics(10);
		// SQLContext sqlContext = new SQLContext(TestMongo.getSparkContext(TestMongo.Collections.TWEETS));

		// StructType schema = new StructType(new StructField[] {
		// new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
		// });
		// DataFrame df = sqlContext.createDataFrame(jrdd, schema);
		//
		// // fit a CountVectorizerModel from the corpus
		// CountVectorizerModel cvModel = new CountVectorizer()
		// .setInputCol("text")
		// .setOutputCol("feature")
		// .setMinDF(2) // a term must appear in more or equal to 2 documents to be included in the vocabulary
		// .fit(df);
		//
		// cvModel.transform(df).show();

	}

	private void createTopics(int k) {

	}

	private static class ParseVector implements Function<String, Row> {
		private static final Pattern separator = Pattern.compile(" ");

		@Override
		public Row call(String line) {
			String[] tok = separator.split(line);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}
			Vector[] points = { Vectors.dense(point) };
			return new GenericRow(points);
		}
	}

}
