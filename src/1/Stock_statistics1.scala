package Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, countDistinct, lit, split, desc}
object Stock_statistics1 {
  def main(args: Array[String]): Unit = {
    //创建spark会话
    val spark = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()
    //设置日志级别
    import  spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    //读取数据集
    var stock_data=spark.read.option("inferSchema", "true").option("header", "true").option("sep",",").csv("/Users/shencongshan/Documents/金融大数据处理技术/实验5/stock_small.csv")
    println("原始数据：")
    stock_data.show(5)
    //提取年份
    stock_data=stock_data.withColumn("year",split(split(col("date")," ")(0),"-")(0).cast("Int"))
    println("提取年份：")
    stock_data.show(5)
    var rest=stock_data.groupBy("year","stock_symbol").sum("stock_volume")
    println("汇总结果：")
    rest.show(5)
    //排序
    rest=rest.orderBy(col("year"),desc("sum(stock_volume)"))
    println("排序结果：")
    rest.show(5)
    rest.repartition(1).rdd.saveAsTextFile("/Users/shencongshan/Documents/金融大数据处理技术/实验5/rest2")
  }
}
