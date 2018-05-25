package me.deviantcode.spark.playground

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object GraphFrames extends App with SparkSessionManager {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  override val logger = LoggerFactory.getLogger(getClass)

  withLoanedSparkSession { spark =>
    go(spark)
  }

  def go(spark: SparkSession) = {
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.graphframes.GraphFrame

    val bikedata = spark.read
      .option("header", "true")
      .csv("src/main/resources/201801-fordgobike-tripdata.csv")
    bikedata.cache()

    complex()

    def basics() = {

      bikedata.show()

      val start_stations = bikedata
        .select(col("start_station_name"))
        .withColumnRenamed("start_station_name", "id")

      val end_stations = bikedata
        .select(col("end_station_name"))
        .withColumnRenamed("end_station_name", "id")

      val stationVertices = start_stations union end_stations distinct()
      stationVertices.cache()
      stationVertices.show(false)

      val tripEdges = bikedata
        .select(col("start_station_name"), col("end_station_name"))
        .withColumnRenamed("start_station_name", "src")
        .withColumnRenamed("end_station_name", "dst")
      tripEdges.show(false)

      // Create Graphs
      val stationGraph = GraphFrame(stationVertices, tripEdges)
      stationGraph.cache()

      println(s"Total stations: ${stationGraph.vertices.count()}")
      println(s"Total trips in Graph: ${stationGraph.edges.count()}")
      println(s"Total trips in Original: ${tripEdges.count()}")


      // basic queries
      stationGraph
        .edges
        .where("src = 'Townsend St at 5th St' OR dst = 'Townsend St at 5th St'")
        .groupBy("src", "dst")
        .count()
        .orderBy(desc("count"))
        .show(10)

      val townStrAt5thEdges = stationGraph
        .edges
        .where("src = 'Townsend St at 5th' OR dst = 'Towsend St at 5th'")

      // create a subgraph
      val subgraph = GraphFrame(stationGraph.vertices, townStrAt5thEdges)

      // run pagerank
      val ranks = stationGraph.pageRank
        .resetProbability(0.15)
        .maxIter(10)
        .run()

      ranks.vertices
        .orderBy(desc("pagerank"))
        .select("id", "pagerank")
        .show(10)

      // indegrees vs out degrees
      val inDeg = stationGraph.inDegrees
        .orderBy(desc("inDegree"))

      inDeg.show(10, false)

      val outDeg = stationGraph.outDegrees
        .orderBy(desc("outDegree"))

      outDeg.show(10, false)

      val degreeRatio = inDeg.join(outDeg, Seq("id"))
        .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

      degreeRatio.orderBy(desc("degreeRatio"))
        .show(10, false)

      degreeRatio.orderBy("degreeRatio")
        .show(10, false)


      //bfs
      val bsfResult = stationGraph.bfs
        .fromExpr("id = 'San Francisco Caltrain (Townsend St at 4th St)'")
        .toExpr("id = 'McAllister St at Baker St'")
        .maxPathLength(5)
        //      .edgeFilter() // filter some aprior condions out
        .run()

      bsfResult.distinct().show(10, false)

    }

    def complex() = {

      val stationVertices = {
        bikedata.select(col("start_station_name")).withColumnRenamed("start_station_name", "id") union
          bikedata.select(col("end_station_name")).withColumnRenamed("end_station_name", "id") distinct()
      }

      val tripEdges = bikedata
        .withColumnRenamed("start_station_name", "src")
        .withColumnRenamed("end_station_name", "dst")

      /*run connected components section*/
      // ccs

      /* run motifs section */
      motifs


      def ccs = {
        // connected components
        // compute intensive so save intermediate steps as checkpoints
        // clear cache before starting to have max memory available.
        spark.sqlContext.clearCache()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")

        //also sample some data for this run
        val minGraph = GraphFrame(stationVertices, tripEdges.sample(false, 0.2))

        val cc = minGraph
          .connectedComponents
          .run()

        cc.show(false)


        // strongly connected components (on directed graphs) -> one way into subgraph but no way out

        val scc = minGraph
          .stronglyConnectedComponents
          .maxIter(3)
          .run()

        scc.show(false)
        scc.groupBy("component").count().show()
        spark.sqlContext.clearCache()
      }

      def motifs = {

        // Mofit Finding
        // find all round trips with two stations in between ie; a -> b -> c -> a
        // (a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)
        // results is a dataframe that can be queried
        val stationGraph = GraphFrame(stationVertices, tripEdges.sample(false, 0.2))
        stationGraph.cache()

        val roundtripMotifs = stationGraph
          .find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")
        //        roundtripMotifs.printSchema()
        roundtripMotifs.show(false)

        // now given a certain bike, find the shortest round trip where that bike was taken from a station(a) to another(b) & dropped off, then picked and
        // ridden to another (c) where its dropped off  and then picked again and ridden to original station(a)

        // hint: simplify dates for comparisons

        roundtripMotifs
          .selectExpr(
            "*",
            "to_timestamp(ab.start_time, 'yyyy-MM-dd HH:mm:ss') as abStart",
            "to_timestamp(bc.start_time, 'yyyy-MM-dd HH:mm:ss') as bcStart",
            "to_timestamp(ca.start_time, 'yyyy-MM-dd HH:mm:ss') as caStart") // convert to timestamps for comparisons
          .where("ab.bike_id = bc.bike_id") //ensure same bike
          .where("bc.bike_id = ca.bike_id")
          .where(expr("a.id != b.id AND b.id != c.id")) // ensure different stations
          .where(expr("abStart < bcStart AND  bcStart < caStart"))
          .withColumn("duration", expr("cast(caStart as long) - cast(abStart as long)")) // endtime - starttime
          .orderBy("duration") // order them all
          .selectExpr("a.id", "b.id", "c.id", "ab.start_time", "ca.end_time", "duration")
          .limit(10)
          .show(false)

      }
    }
  }
}
