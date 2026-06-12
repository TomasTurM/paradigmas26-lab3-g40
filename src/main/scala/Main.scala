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
    sc.setLogLevel("WARN")
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

    // a) Agreguen los siguientes Accumulators al pipeline:
    val feedsSuccessAcc = sc.longAccumulator("feedsSuccess")
    val feedsFailedAcc = sc.longAccumulator("feedsFailed")
    val postsDownloadedAcc = sc.longAccumulator("postsDownloaded")
    val postsFailedAcc = sc.longAccumulator("postsFailed") // Para feeds con 0 posts
    val postsDiscardedAcc = sc.longAccumulator("postsDiscarded")

    // Download feeds and parse posts, tracking success/failure
    val downloadResults = subscriptionsRDD.map{ subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      if (feedOpt.isDefined) feedsSuccessAcc.add(1) else feedsFailedAcc.add(1)
      if (feedOpt.isEmpty) {
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
      }
      val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name, subscription.url))
      postsDownloadedAcc.add(posts.length)
      if (posts.isEmpty) postsFailedAcc.add(1)
      (feedOpt.isDefined, posts)
    }

    // Flatten all posts and count JSON parse failures
    val allPosts = downloadResults.flatMap(_._2)// en este punto llego a RDD[Post]

    // Filter empty posts
    val filteredPostsRDD = allPosts.filter { post =>
      val isValid = post.title.nonEmpty &&
        post.selftext.nonEmpty &&
        post.selftext.trim.nonEmpty
      if (!isValid) postsDiscardedAcc.add(1)
      isValid
    }.cache()
    //RDD[Post] (filtrados)

    // c) Midan el tiempo total de ejecución del pipeline
    val startTime1 = System.currentTimeMillis()

    // Acción terminal para disparar el pipeline y poblar los acumuladores
    val countedFilteredPosts = filteredPostsRDD.count()

    val endTime1 = System.currentTimeMillis()
    val duration1 = (endTime1 - startTime1) / 1000.0

    // Check si no tenemos posts para procesar
    if (countedFilteredPosts == 0) {
      filteredPostsRDD.unpersist()
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Calculate average characters in filtered posts
    val totalChars = filteredPostsRDD.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (countedFilteredPosts > 0) totalChars / countedFilteredPosts else 0

    // b) Utilicen estos acumuladores para calcular las estadísticas
    val stats = Map(
      "feedsSuccess" -> feedsSuccessAcc.value.toInt,
      "feedsFailed" -> feedsFailedAcc.value.toInt,
      "postsSuccess" -> postsDownloadedAcc.value.toInt,
      "postsFailed" -> postsFailedAcc.value.toInt,
      "postsFiltered" -> postsDiscardedAcc.value.toInt,
      "avgChars" -> avgChars.toInt
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println(s"Tiempo de procesamiento de feeds y filtrado: ${duration1} segundos")
    println()

    // c) Medir el tiempo de la segunda acción terminal
    val startTime2 = System.currentTimeMillis()

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

    val endTime2 = System.currentTimeMillis()
    val duration2 = (endTime2 - startTime2) / 1000.0

    println(Formatters.formatAllEntities(sortedAllEntities))
    println(s"Tiempo de análisis de entidades: ${duration2} segundos")
    println()

    filteredPostsRDD.unpersist()

  }
}
