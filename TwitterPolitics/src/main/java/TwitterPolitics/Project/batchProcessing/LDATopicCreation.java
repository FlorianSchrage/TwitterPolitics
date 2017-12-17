/**
 * 
 */
package TwitterPolitics.Project.batchProcessing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author Steffen Terheiden
 *
 */
public class LDATopicCreation {

	public static void main(String[] args) {
		int NUMBER_OF_TOPICS = 10;
		int NUMBER_OF_WORDS_TO_DESCRIBE_A_TOPIC = 5;

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

		DataFrame wordsDataFrame = tokenizer.transform(sentenceDataFrame).drop("sentence");
		wordsDataFrame.show();

		CountVectorizerModel cvModel = new CountVectorizer()
				.setInputCol("words")
				.setOutputCol("features")
				.setMinDF(2)
				.fit(wordsDataFrame);
		String[] vocabulary = cvModel.vocabulary();
		Arrays.stream(vocabulary).forEach(s -> System.out.println("Vocabulary: " + s));
		DataFrame featureDataFrame = cvModel.transform(wordsDataFrame);
		featureDataFrame.show();

		// Trains a LDA model
		LDA lda = new LDA()
				.setK(NUMBER_OF_TOPICS)
				.setMaxIter(30);
		LDAModel model = lda.fit(featureDataFrame);

		// performance evaluation
		// System.out.println("logLikelihood: " + model.logLikelihood(featureDataFrame));
		// System.out.println("logPerplexity: " + model.logPerplexity(featureDataFrame));

		// TODO: set reasonable maximum number, get them from featue data frame ...
		int maxNumberOfTerms = 250;
		DataFrame topics = model.describeTopics(maxNumberOfTerms);
		HashMap<String, JSONArray> wordsWithTopicRelatedValues = new HashMap<>();
		HashMap<Integer, List<String>> topicsWithDescription = new HashMap<>();

		topics.takeAsList(NUMBER_OF_TOPICS).forEach(r -> {
			// System.out.println("Indices: " + r.getList(1)); // get indices, 2 ->> get weights
			List<Integer> indices = r.getList(1);// get indices
			List<Double> weights = r.getList(2);// get weights (double)
			if (weights.size() != indices.size()) {
				System.out.println("ERROR: Something went wrong. Indices and weights should be of same length.");
			}
			for (int i = 0; i < indices.size(); i++) {
				String word = vocabulary[indices.get(i)];
				double weight = weights.get(i);
				// System.out.println("Word: " + word + " Weight: " + weight + " Topic: " + r.get(0));
				JSONArray weightsForTopics = wordsWithTopicRelatedValues.get(word);
				if (weightsForTopics != null) {
					wordsWithTopicRelatedValues.put(word, weightsForTopics.put(new JSONObject().put(Integer.toString((int) r.get(0)), weight)));
				} else {
					wordsWithTopicRelatedValues.put(word, new JSONArray().put(new JSONObject().put(Integer.toString((int) r.get(0)), weight)));
				}

				if (i < NUMBER_OF_WORDS_TO_DESCRIBE_A_TOPIC) {
					List<String> listOfWords = topicsWithDescription.get((int) r.get(0));
					if (listOfWords == null) {
						listOfWords = new ArrayList<>();
					}
					listOfWords.add(word);
					topicsWithDescription.put((int) r.get(0), listOfWords);

				}

			}
		});
		TestMongo.saveToMongo(wordsWithTopicRelatedValues, TestMongo.Collections.RESULTS);
		TestMongo.saveTopicsToMongo(topicsWithDescription);

		topics.show(false);
		// model.transform(featureDataFrame).show(false);
		TestMongo.printAllData(TestMongo.Collections.RESULTS);
		System.out.println();
		TestMongo.printAllData(TestMongo.Collections.TOPICS);
	}

}
