package Stock

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object Stock_statistics4 {
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
    //过滤出苹果公司的数据
    stock_small=stock_small.filter("stock_symbol=='AAPL'")
    println("苹果公司数据：")
    stock_small.show(5)
    //提取年
    stock_small=stock_small.withColumn("year",split(split(col("date")," ")(0),"-")(0).cast("Int"))
    println("提取年份数据：")
    stock_small.show(5)
    //计算每年的交易天数
    stock_small.registerTempTable("t")
    val trans_date_num=spark.sql("select year,count(distinct date) as date_num from  t group by year")
    println("每年交易天数：")
    trans_date_num.show()
    //计算每年总的调整后收盘价总和
    val price_adj_sum=spark.sql("select year,sum(stock_price_adj_close) as price_adj_sum from t group by year")
    println("每年总的调整后收盘价总和：")
    price_adj_sum.show()
    //关联每年总的调整后收盘价总和和每年的交易天数
    val full_data=price_adj_sum.join(trans_date_num,Array("year"),"inner")
    println("每年调整后收盘价总和和交易天数表：")
    full_data.show()
    //计算年平均调整后收盘价
    val rest=full_data.withColumn("mean_stock_price_adj_close",col("price_adj_sum")/col("date_num"))
    //过滤出年平均调整后收盘价大于50的年份
    val rest2=rest.filter("mean_stock_price_adj_close>50").select("year","mean_stock_price_adj_close")
    println("年平均调整后收盘价大于50的年份")
    rest2.show()
    rest2.repartition(1).rdd.saveAsTextFile("/Users/shencongshan/Documents/金融大数据处理技术/实验5/rest5")


  }
}
