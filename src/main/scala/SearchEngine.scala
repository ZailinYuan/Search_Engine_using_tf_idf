import org.apache.spark.{SparkConf, SparkContext}

object SearchEngine {
  def main(args: Array[String]): Unit = {

    // Input paths:
    val plot_summaries_path = "C:\\Users\\zailinyuan\\IdeaProjects\\SearchEngine\\file\\plot_summaries.txt"
    val stop_words_path = "C:\\Users\\zailinyuan\\IdeaProjects\\SearchEngine\\file\\stopwords.txt"
    val search_term_path = "C:\\Users\\zailinyuan\\IdeaProjects\\SearchEngine\\file\\search_terms.txt"
    val multiple_search_path = "C:\\Users\\zailinyuan\\IdeaProjects\\SearchEngine\\file\\multiple_search_terms.txt"

    // Environment:
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Search Movies")
    val sc = new SparkContext(sparkConf)

    // Load data:
    val plot_summaries = sc.textFile(plot_summaries_path)
    val stop_words = sc.textFile(stop_words_path)
    val search_terms = sc.textFile(search_term_path)
    val multiple = sc.textFile(multiple_search_path)

    // Remove stop words:
    val stopWordsSet = stop_words.collect().toSet
    val clean_plot_summaries = plot_summaries
      .map(_.replaceAll("""[\p{Punct}]|\s+""", " ").toLowerCase().split("\\s+"))
      .map(_.filter(s => !stopWordsSet.contains(s)))
    val clean_search_terms = search_terms.flatMap(_.split("\\s+")).filter(x => !stopWordsSet.contains(x))
      .map(_.toLowerCase())
    val clean_multi_term = multiple.flatMap(x => x.split("""\s+""")).filter(x => !stopWordsSet.contains(x))

    // Calculate TFs:
    val termMoviePairs = clean_search_terms.cartesian(clean_plot_summaries)
    val pairsTF = termMoviePairs.map(x => ((x._1, x._2(0)), x._2.count(_.equals(x._1)).toDouble / x._2.size))
      .filter(_._2!=0.0)

    //pairsTF.collect().foreach(println(_))

    // Calculate IDFs:
    val size = plot_summaries.count()
    val countTerm = pairsTF.map(x => (x._1._1, 1)).reduceByKey(_+_)
    val IDFs = countTerm.map(x => (x._1, Math.log(size / (1+x._2))))
    val idf_map = IDFs.collect().toMap

    // IDFs.collect().foreach(println(_))

    // Calculate tf-idf:
    val tf_idf = pairsTF.map(x =>  (x._1._1, (x._2 * idf_map.get(x._1._1).get, x._1._2)))

    // tf_idf.collect().foreach(println(_))

    // Top 5 for every search term:
    val iter = search_terms.collect().toArray.iterator
    while(iter.hasNext) {
      val term = iter.next()
      tf_idf.filter(_._1.equals(term)).sortBy(-_._2._1).take(5).foreach(println(_))
    }


    // Following is the multiple term search using cosine similarity:
    
  }
}
