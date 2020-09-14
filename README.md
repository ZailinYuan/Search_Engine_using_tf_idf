# Search_Engine_using_tf_idf
tf_idf algorithm as a search engine

* Spark sbt based

* Single term search
  * The input search terms are stored in ./file/search_terms.txt. Each row is treated as a single term search.
  * Documents searched are stored in ./file/plot_summaries.txt. Take 5 movies with highest td-idf value as search results.
  * Outputs:
  <image src="images/moviesRank1.PNG" width=%40 />
  <image src="images/moviesRank2.PNG" width=%40 />
  <image src="images/moviesRank3.PNG" width=%40 />
  <image src="images/moviesRank4.PNG" width=%40 />
  

* tf-idf equations:
<image src="images/tf.jpg" width=%40 />
<image src="images/idf.jpg" width=%40 />
<image src="images/tf_idf.png" width=%40 />
