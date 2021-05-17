
import java.io._
import org.apache.spark._


object testingsort extends App {
  val conf = new SparkConf().setAppName("Spark Sort").setMaster("local")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  var myOne = List(List("cat", "mat", "bat"), List("hat", "mat", "rat"), List("cat", "mat", "sat"), List("cat", "fat", "bat"))
  myOne.foreach(println)
  var myTwo = sc.parallelize(myOne)
  myTwo.foreach(println)
  var myThree = myTwo.map(x => (x(1), x(2)))
  myThree.foreach(println)
  var myFour = myThree.sortBy(_._1, false).sortBy(_._2)
  myFour.foreach(println)

  val writer = new PrintWriter(new File("result.txt"))


  writer.write("Hello World")

  var tf = sc.textFile("data/dataFile.txt")
  var counts = tf.flatMap(line => line.split("\\s+")).map(x => (x.toLowerCase(), 1)).reduceByKey(_ + _).sortByKey(true)
  counts.foreach(println)

  val res = counts.collect()
  for (n <- res) writer.println(n.toString())

  writer.close()


  val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

  val dfs = sqlcontext.read.json("data/Emp.json")

  dfs.show()
  dfs.printSchema()
  val dfs1 = dfs.filter(!dfs("name").isNull)
  dfs1.show()
  dfs1.select("name").show()
  dfs1.filter(dfs("age") > 23).show()
  dfs1.groupBy("age").count().show()

}
