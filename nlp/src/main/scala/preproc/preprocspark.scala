import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object PreprocSpark {
  def main(args: Array[String]): Unit = {

    if (args.size < 2) {
      println("Arguments: input_dir output_dir [min_num_partitions=5]")
      System.exit(1)
    }

    val input_dir = args(0)
    val output_dir = args(1)
    val min_num_partitions = if (args.length >= 3) {
      args(2).toInt
    } else {
      5
    }

    // Create Spark session and context
    val spark_session = SparkSession
      .builder()
      .appName("Preproc Spark")
      .getOrCreate()

    import spark_session.implicits._

    // Load input data, create schema defintion and dataframe
    val spark_context = spark_session.sparkContext
    val textfiles_rdd: RDD[(String, String)] = spark_context.wholeTextFiles(input_dir, minPartitions = min_num_partitions).cache
    val num_files = textfiles_rdd.count()
    val zippedWithIndexRdd: RDD[((String, String), Long)] = textfiles_rdd.zipWithIndex
    val textfiles_rows: RDD[Row] = zippedWithIndexRdd.map(pair => Row(pair._2, pair._1._2.replaceAll("<[^>]*>", "")))
    val df_schema = StructType(Seq(StructField("index", LongType, true), StructField("text", StringType, true)))
    val textfiles_df = spark_session.createDataFrame(textfiles_rows, df_schema)

     textfiles_df.show()

    // Execute NLP Pipeline
     val output = textfiles_df
       .select('index, explode(ssplit('text)).as('sen))
       .select('index, 'sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

    output.show(truncate = false)

    val regex_singlechar = "^[b-zB-Z]$"
    val regex_notword = "[^a-zA-Z]"
    val regex_url = "http\\S+"
    val regex_tags = "<[^>]*>"

    //val stop_words: Set[String] = Set("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "lrb", "re", "rrb", "replyto")
    val filteredOutputDF = output.select("index", "words", "nerTags", "sentiment")
       .map({ case Row(index: Long, words: Seq[String], ner: Seq[String], sentiment:Int) => (index, words zip ner, sentiment) })
       .map { case (index: Long, pairs, sentiment:Int) => {
         val line = pairs.map { pair =>
           val result = pair._1
             .replaceAll(regex_tags, "")
             .replaceAll(regex_url, "")
             .replaceAll(regex_notword, "")
             .replaceAll(regex_singlechar, "")
             .toLowerCase
           (result, pair._2)
         }//.filter { pair =>
           //pair._2 != "O"
         //}
         .map { pair =>
           pair._1 + "|" + pair._2
         }.mkString(" ")

         val lineWithSentiment = line+ " [" +sentiment+"] "
         (index, lineWithSentiment)
        }

       }.filter(_._2 != "")

    val filteredOutputRDD: RDD[(Long, String)] = filteredOutputDF.rdd

    object filePartitioner extends Partitioner {
      override def numPartitions: Int = num_files.toInt

      override def getPartition(key: Any): Int = (key.asInstanceOf[Long].toInt)
    }

    val linesByFileRDD = filteredOutputDF
      .rdd.reduceByKey(filePartitioner, (a, b) => a + "\n" + b).values
    linesByFileRDD.saveAsTextFile(output_dir)
  }
}
