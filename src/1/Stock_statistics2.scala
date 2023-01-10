package Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, split,round}
object Stock_statistics2 {
  def main(args: Array[String]): Unit = {
    //创建spark会话
    val spark = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()
    //设置日志级别
    spark.sparkContext.setLogLevel("ERROR")
    //读取数据集
    var stock_data=spark.read.option("inferSchema", "true").option("header", "true").option("sep",",").csv("/Users/shencongshan/Documents/金融大数据处理技术/实验5/stock_small.csv")
    println("原始数据：")
    stock_data.show(5)
    stock_data=stock_data.withColumn("date",split(col("date")," ")(0))
    //计算stock_price_close与stock_price_open差价
    stock_data=stock_data.withColumn("diff",col("stock_price_close")-col("stock_price_open")).withColumn("diff",round(col("diff"),4))
    println("差价计算结果：")
    stock_data.show(5)
    //对差价排序
    val rest=stock_data.orderBy(desc("diff")).limit(10)
    //显示
    println("差价最大的前十：")
    rest.show()
    rest.select("exchange","stock_symbol","date","stock_price_close","stock_price_open","diff").repartition(1).rdd.saveAsTextFile("/Users/shencongshan/Documents/金融大数据处理技术/实验5/rest3")
  }
}
