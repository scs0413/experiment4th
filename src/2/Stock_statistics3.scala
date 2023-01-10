package Stock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, split}

object Stock_statistics3 {
  def main(args: Array[String]): Unit = {
    //创建spark会话
    val spark = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()
    //设置日志级别
    spark.sparkContext.setLogLevel("ERROR")
    //读取数据集
    var stock_small=spark.read.option("inferSchema", "true").option("header", "true").option("sep",",").csv("/Users/shencongshan/Documents/金融大数据处理技术/实验5/stock_small.csv")
    println("stock_small数据：")
    stock_small.show(5)
    var dividends_small=spark.read.option("inferSchema", "true").option("header", "true").option("sep",",").csv("/Users/shencongshan/Documents/金融大数据处理技术/实验5/dividends_small.csv")
    println("dividends_small数据：")
    dividends_small=dividends_small.withColumn("stock_symbol",col("symbol"))
    dividends_small.show(5)
    //过滤出IBM的数据
    stock_small=stock_small.filter("stock_symbol=='IBM'")
    //关联两张表
    val rest=stock_small.join(dividends_small,Array("exchange","stock_symbol","date"),"inner").select("date","stock_symbol","stock_price_close").withColumn("date",split(col("date")," ")(0))
    rest.repartition(1).rdd.saveAsTextFile("/Users/shencongshan/Documents/金融大数据处理技术/实验5/rest4")
  }
}
