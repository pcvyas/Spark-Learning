import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Just fooling around with Scala, followed an online tutorial
   that showed me how to  do basic data manipulation with Spark
 */
object ScalaApp {

  def main(args: Array[String]) {

  	//2 threads running
    val sc = new SparkContext("local[2]", "Intro to SparkContext")

    // initialize the text/csv file as an RDD and convert it to this form (customer, product, price)
    val data = sc.textFile("data/UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    // count total purchases
    val numPurchases = data.count()

    // creating a case class and mapping it so that it is only the customer, then doing a simple distinct and count
    val uniqueCustomers = data.map { case (customer, product, price) => customer }.distinct().count()

    // adding all the money that the customers spent
    val totalRevenue = data.map { case (customer, product, price) => price.toDouble }.sum()

    //groupByKey sucks!

    val productsByPopularity = data
      .map { case (customer, product, price) => (product, 1) }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)
    val mostPopular = productsByPopularity(0)

    // Print out everything in a decent format
    println("Total purchases: " + numPurchases)
    println("Unique Customers: " + uniqueCustomers)
    println("Total revenue: " + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

    //stop the spark context
    sc.stop()
  }

}
