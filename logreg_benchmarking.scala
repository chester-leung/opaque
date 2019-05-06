import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import edu.berkeley.cs.rise.opaque.expressions.DotProduct.dot
import edu.berkeley.cs.rise.opaque.Utils



/** Expand a categorical column into multiple columns, one for each category. */
def getDummies(d: DataFrame, col: String): DataFrame = {
  // Get all distinct categories for the column
  val categories = d.select(col).distinct.collect.map(_.getString(0))
  // Create a new column for each category containing an indicator variable
  val expanded = categories.foldLeft(d) { (d2, c) =>
    d2.withColumn(
      s"${col}_$c",
      when(new Column(col) === c, 1).otherwise(0))
  }
  // Drop the categorical column
  expanded.drop(col)
}

Utils.timeBenchmark("case" -> "plaintext no cache") {
  val dir = "/sbnda/data/synthetic"
  val table1 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz")
  val table2 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz")
  val table3 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz")
  val joined = table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF")

  val withCategories = getDummies(getDummies(joined, "ATRR_76"), "ATRR_7")
  val data = withCategories.select(
    $"DEF_IND".cast(IntegerType).as("y_truth"),
    array(
      $"ATRR_35".cast(DoubleType),
      $"ATRR_36".cast(DoubleType),
      $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
      .as("x"))

  // Pre-trained logistic regression model from sklearn
  // Model coefficients
  val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
  // Model intercept
  val b = lit(-7.20533994)

  val predictions = data.select(
    $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

  // Count the number of correct predictions. Expected: 955543 out of 1000000
  val numCorrect = predictions.filter(
    $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
      $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count
  
  println(f"Correct Predictions: $numCorrect / 1,000,000")
}

Utils.timeBenchmark("case" -> "plaintext") {
  var start = System.nanoTime
  val dir = "/sbnda/data/synthetic"
  println("table 1")
  val table1 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz").select(
      $"DEF_IND".cast(IntegerType),
      $"ATRR_76".cast(DoubleType),
      ))
  Utils.force(table1)
  println("table 2")
  val table2 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz").select(
      $"DEF_IND".cast(IntegerType),
      $"ATRR_76".cast(DoubleType),
      ))
  Utils.force(table2)
  println("table 3")
  val table3 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz").select(
      $"DEF_IND".cast(IntegerType),
      $"ATRR_36".cast(DoubleType),
      $"ATRR_76".cast(DoubleType),
      $"ATRR_7"))
  Utils.force(table3)
  var time = (System.nanoTime - start) / 1000000.0
  println(f"load data took $time%2.2f ms")
  
  start = System.nanoTime
  val joined = Utils.ensureCached(table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF"))
  Utils.force(joined)
  time = (System.nanoTime - start) / 1000000.0
  println(f"join took $time%2.2f ms")

  start = System.nanoTime
  val withCategories = Utils.ensureCached(getDummies(getDummies(joined, "ATRR_76"), "ATRR_7"))
  Utils.force(withCategories)
  time = (System.nanoTime - start) / 1000000.0
  println(f"one hot encoding took $time%2.2f ms")
  
  start = System.nanoTime
  val data = withCategories.select(
    $"DEF_IND".cast(IntegerType).as("y_truth"),
    array(
      $"ATRR_35".cast(DoubleType),
      $"ATRR_36".cast(DoubleType),
      $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
      $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
      .as("x"))

  // Pre-trained logistic regression model from sklearn
  // Model coefficients
  val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
  // Model intercept
  val b = lit(-7.20533994)

  val predictions = data.select(
    $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

  // Count the number of correct predictions. Expected: 955543 out of 1000000
  val numCorrect = predictions.filter(
    $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
      $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

  time = (System.nanoTime - start) / 1000000.0
  println(f"inference took $time%2.2f ms")

  println(f"Correct Predictions: $numCorrect / 1,000,000")

}

// Utils.timeBenchmark("case" -> "encrypted inference") {
//   var start = System.nanoTime
//   val dir = "/sbnda/data/synthetic"
//   val table1 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz"))
//   Utils.force(table1)
//   val table2 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz"))
//   Utils.force(table2)
//   val table3 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz"))
//   Utils.force(table3)
//   var time = (System.nanoTime - start) / 1000000.0
//   println(f"load data took $time%2.2f ms")
  
//   start = System.nanoTime
//   val joined = Utils.ensureCached(table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF"))
//   Utils.force(joined)
//   time = (System.nanoTime - start) / 1000000.0
//   println(f"join took $time%2.2f ms")

//   start = System.nanoTime
//   val withCategories = Utils.ensureCached(getDummies(getDummies(joined, "ATRR_76"), "ATRR_7"))
//   Utils.force(withCategories)
//   time = (System.nanoTime - start) / 1000000.0
//   println(f"one hot encoding took $time%2.2f ms")
  
//   start = System.nanoTime
//   val data = withCategories.select(
//     $"DEF_IND".cast(IntegerType).as("y_truth"),
//     array(
//       $"ATRR_35".cast(DoubleType),
//       $"ATRR_36".cast(DoubleType),
//       $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//       .as("x")).encrypted

//   // Pre-trained logistic regression model from sklearn
//   // Model coefficients
//   val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//   // Model intercept
//   val b = lit(-7.20533994)

//   val predictions = data.select(
//     $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//   // Count the number of correct predictions. Expected: 955543 out of 1000000
//   val numCorrect = predictions.filter(
//     $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//       $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

//   time = (System.nanoTime - start) / 1000000.0
//   println(f"inference took $time%2.2f ms")

//   println(f"Correct Predictions: $numCorrect / 1,000,000")
// }

// Utils.timeBenchmark("case" -> "encrypted pipeline") {
//   var start = System.nanoTime
//   val dir = "/sbnda/data/synthetic"
//   val table1 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz").repartition(30).encrypted)
//   Utils.force(table1)
//   val table2 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz").repartition(30).encrypted)
//   Utils.force(table2)
//   val table3 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz").repartition(30).encrypted)
//   Utils.force(table3)
//   var time = (System.nanoTime - start) / 1000000.0
//   println(f"load data took $time%2.2f ms")
  
//   start = System.nanoTime
//   val joined = Utils.ensureCached(table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF"))
//   Utils.force(joined)
//   time = (System.nanoTime - start) / 1000000.0
//   println(f"join took $time%2.2f ms")

//   start = System.nanoTime
//   val withCategories = Utils.ensureCached(getDummies(getDummies(joined, "ATRR_76"), "ATRR_7"))
//   Utils.force(withCategories)
//   time = (System.nanoTime - start) / 1000000.0
//   println(f"one hot encoding took $time%2.2f ms")
  
//   start = System.nanoTime
//   val data = withCategories.select(
//     $"DEF_IND".cast(IntegerType).as("y_truth"),
//     array(
//       $"ATRR_35".cast(DoubleType),
//       $"ATRR_36".cast(DoubleType),
//       $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//       .as("x"))

//   // Pre-trained logistic regression model from sklearn
//   // Model coefficients
//   val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//   // Model intercept
//   val b = lit(-7.20533994)

//   val predictions = data.select(
//     $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//   // Count the number of correct predictions. Expected: 955543 out of 1000000
//   val numCorrect = predictions.filter(
//     $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//       $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

//   time = (System.nanoTime - start) / 1000000.0
//   println(f"inference took $time%2.2f ms")

//   println(f"Correct Predictions: $numCorrect / 1,000,000")
// }



// Utils.timeBenchmark("case" -> "encrypted inference") {
//   Utils.timeBenchmark("case" -> "encrypted inference", "step" -> "load data") {
//     val dir = "/sbnda/data/synthetic"
//     val table1 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz"))
//     Utils.force(table1)
//     val table2 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz"))
//     Utils.force(table2)
//     val table3 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz"))
//     Utils.force(table3)
//   }

//   Utils.timeBenchmark("case" -> "encrypted inference", "step" -> "join") {
//     val joined = Utils.ensureCached(table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF"))
//     Utils.force(joined)
//   }
  
//   Utils.timeBenchmark("case" -> "encrypted inference", "step" -> "one hot encoding") {
//     val withCategories = Utils.ensureCached(getDummies(getDummies(joined, "ATRR_76"), "ATRR_7"))
//     Utils.force(withCategories)
//   }

//   Utils.timeBenchmark("case" -> "encrypted inference", "step" -> "inference") {
//     val data = withCategories.select(
//       $"DEF_IND".cast(IntegerType).as("y_truth"),
//       array(
//         $"ATRR_35".cast(DoubleType),
//         $"ATRR_36".cast(DoubleType),
//         $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//         .as("x")).encrypted

//     // Pre-trained logistic regression model from sklearn
//     // Model coefficients
//     val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//     // Model intercept
//     val b = lit(-7.20533994)

//     val predictions = data.select(
//       $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//     // Count the number of correct predictions. Expected: 955543 out of 1000000
//     val numCorrect = predictions.filter(
//       $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//         $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

//     println(f"Correct Predictions: $numCorrect / 1,000,000")
//   }
// }

// Utils.timeBenchmark("case" -> "end to end encrypted pipeline") {
//   Utils.timeBenchmark("case" -> "encrypted pipeline", "step" -> "load data") {
//     val dir = "/sbnda/data/synthetic"
//     val table1 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz").repartition(30).encrypted)
//     Utils.force(table1)
//     val table2 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz").repartition(30).encrypted)
//     Utils.force(table2)
//     val table3 = Utils.ensureCached(spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz").repartition(30).encrypted)
//     Utils.force(table3)
//   }

//   Utils.timeBenchmark("case" -> "encrypted pipeline", "step" -> "join") {
//     val joined = Utils.ensureCached(table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF"))
//     Utils.force(joined)
//   }
  
//   Utils.timeBenchmark("case" -> "encrypted pipeline", "step" -> "one hot encoding") {
//     val withCategories = Utils.ensureCached(getDummies(getDummies(joined, "ATRR_76"), "ATRR_7"))
//     Utils.force(withCategories)
//   }

//   Utils.timeBenchmark("case" -> "encrypted pipeline", "step" -> "inference") {
//     val data = withCategories.select(
//       $"DEF_IND".cast(IntegerType).as("y_truth"),
//       array(
//         $"ATRR_35".cast(DoubleType),
//         $"ATRR_36".cast(DoubleType),
//         $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//         $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//         .as("x"))

//     // Pre-trained logistic regression model from sklearn
//     // Model coefficients
//     val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//     // Model intercept
//     val b = lit(-7.20533994)

//     val predictions = data.select(
//       $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//     // Count the number of correct predictions. Expected: 955543 out of 1000000
//     val numCorrect = predictions.filter(
//       $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//         $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

//     println(f"Correct Predictions: $numCorrect / 1,000,000")
//   }
// }

// Utils.timeBenchmark("case" -> "encrypted inference") {
//   val dir = "/sbnda/data/synthetic"
//   val table1 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz")
//   val table2 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz")
//   val table3 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz")
//   val joined = table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF")

//   val withCategories = getDummies(getDummies(joined, "ATRR_76"), "ATRR_7")
//   val data = withCategories.select(
//     $"DEF_IND".cast(IntegerType).as("y_truth"),
//     array(
//       $"ATRR_35".cast(DoubleType),
//       $"ATRR_36".cast(DoubleType),
//       $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//       .as("x")).encrypted

//   // Pre-trained logistic regression model from sklearn
//   // Model coefficients
//   val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//   // Model intercept
//   val b = lit(-7.20533994)

//   val predictions = data.select(
//     $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//   // Count the number of correct predictions. Expected: 955543 out of 1000000
//   val numCorrect = predictions.filter(
//     $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//       $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count
  
//   println(f"Correct Predictions: $numCorrect / 1,000,000")
// }

// Utils.timeBenchmark("case" -> "end to end encrypted pipeline") {
//   val dir = "/sbnda/data/synthetic"
//   val table1 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table1.csv.gz").repartition(30).encrypted
//   val table2 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table2.csv.gz").repartition(30).encrypted
//   val table3 = spark.read.format("csv").option("header", "true").load(s"$dir/riselab_table3.csv.gz").repartition(30).encrypted
//   val joined = table1.join(table2.drop("DEF_IND").join(table3.drop("DEF_IND"), "CUST_REF"), "CUST_REF").drop("CUST_REF")

//   val withCategories = getDummies(getDummies(joined, "ATRR_76"), "ATRR_7")
//   val data = withCategories.select(
//     $"DEF_IND".cast(IntegerType).as("y_truth"),
//     array(
//       $"ATRR_35".cast(DoubleType),
//       $"ATRR_36".cast(DoubleType),
//       $"ATRR_76_cat_atrr_76_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_000".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_001".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_003".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_004".cast(DoubleType),
//       $"ATRR_7_cat_atrr_7_005".cast(DoubleType))
//       .as("x"))

//   // Pre-trained logistic regression model from sklearn
//   // Model coefficients
//   val w = array(lit(1.08256682),  lit(0.85389337), lit(-0.30765509),  lit(3.26552064),  lit(2.43142285), lit(-2.35156746), lit(-4.00728388), lit(-5.74680168))
//   // Model intercept
//   val b = lit(-7.20533994)

//   val predictions = data.select(
//     $"y_truth", (lit(1.0) / (lit(1.0) + exp(-(dot(w, $"x") + b)))).as("prediction"))

//   // Count the number of correct predictions. Expected: 955543 out of 1000000
//   val numCorrect = predictions.filter(
//     $"y_truth" === lit(0) && $"prediction" < lit(0.5) ||
//       $"y_truth" === lit(1) && $"prediction" >= lit(0.5)).count

//   println(f"Correct Predictions: $numCorrect / 1,000,000")
// }
