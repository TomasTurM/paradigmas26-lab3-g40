import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    val spark = SparkSession.builder() 
      .appName("RedditNER") 
      .master("local[*]") 
      .getOrCreate() 
    val sc = spark.sparkContext
    //Hasta aqui no cambia nada, solo tengo la spark session creada y el spark context esta disponible
    //Podria hacerlo antes pero eso seria inicializar la session innecesariamente si no se puede parsear el json


    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {//error4
      println("Error: No valid subscriptions found")
      return
    }

    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = subscriptionsRDD.map{ subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      if (feedOpt.isEmpty) {
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
      }
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name, subscription.url))
      (feedOpt.isDefined, posts)
    }

    // Count feed successes/failures
    val feedsSuccess = downloadResults.filter(_._1).count()
    val feedsFailed = downloadResults.count() - feedsSuccess

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResults.flatMap(_._2)// en este punto llego a RDD[Post]
    val postsSuccess = allPosts.count()
    val postsFailed = downloadResults.filter(_._2.isEmpty).count()

    // Filter empty posts
    val filteredPostsRDD = allPosts.filter { post =>
      post.title.nonEmpty &&
      post.selftext.nonEmpty &&
      post.selftext.trim.nonEmpty
    }//RDD[Post] (filtrados)

    // Check si no tenemos posts para procesar
    if (filteredPostsRDD.isEmpty()) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    val countedFilteredPosts = filteredPostsRDD.count()

    val postsFiltered = allPosts.count() - countedFilteredPosts

    // Calculate average characters in filtered posts
    val totalChars = filteredPostsRDD.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (countedFilteredPosts > 0) totalChars / countedFilteredPosts else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess.toInt,
      "feedsFailed" -> feedsFailed.toInt,
      "postsSuccess" -> postsSuccess.toInt,
      "postsFailed" -> postsFailed.toInt,
      "postsFiltered" -> postsFiltered.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    val sortedAllEntities = allEntities.map{ entity =>
      ((entity.entityType, entity.text) -> 1)
    }.reduceByKey(_ + _)
    .sortBy{case ((entityType, text), count) =>
      // Se ordena por conteo descendente y por tipo
      (-count, entityType)
    }.collect().toList

    println(Formatters.formatAllEntities(sortedAllEntities))
    println()

  }
}
