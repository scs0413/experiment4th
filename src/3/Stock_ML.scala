package Stock

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, countDistinct, lit, split, substring}
import org.apache.spark.ml.classification.{DecisionTreeClassifier,LogisticRegression}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
object Stock_ML {
  //主函数
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
    var stock_data=spark.read.option("inferSchema", "true").option("header", "true").option("sep",",").csv("/Users/shencongshan/Documents/金融大数据处理技术/实验5/stock_data.csv")
    println("原始数据：")
    stock_data.show(5)
    //提取月份特征
    stock_data=stock_data.withColumn("Month",split(split(col("date")," ")(0),"-")(1).cast("Int"))
    println("提取月份特征")
    stock_data.show(5)
    //对stock_symbol列进行浓度编码，即在该值内，涨的数量/该值内总数量
    //数据注册为临时表
    stock_data.registerTempTable("t")
    val stock_symbol=spark.sql("select stock_symbol,sum(label)/count(1) as stock_symbol_code from t group by stock_symbol")
    //stock_symbol编码表
    println("stock_symbol编码表")
    stock_symbol.show(5)
    //对数据集中的stock_symbol字段进行转换
    stock_data=stock_data.join(stock_symbol,Array("stock_symbol"),"inner")
    println("stock_symbol编码后的数据")
    stock_data.show(5)
    //与stock_symbol同样的方式，对exchange进行浓度编码
    val exchange=spark.sql("select exchange,sum(label)/count(1) as exchange_code from t group by exchange")
    //exchange编码表
    println("exchange编码表")
    exchange.show(5)
    //对数据集中的exchange字段进行转换
    stock_data=stock_data.join(exchange,Array("exchange"),"inner")
    println("exchange编码后的数据")
    stock_data.show(5)
    //选取特征列表
    val feature_cols=Array("stock_price_open","stock_price_high","stock_price_low","Month","stock_symbol_code","exchange_code")
    //特征组合为向量
    val features = new VectorAssembler().setInputCols(feature_cols).setOutputCol("features")
    var data_set=features.transform(stock_data)
    println("原始数据集数量：",data_set.count())
    //删除缺失值的数据
    data_set=data_set.na.drop()
    println("删除空值之后数据集数量：",data_set.count())
    //将数据按照8：切分为训练集和测试集
    val split_data=data_set.randomSplit(Array(0.8,0.2))
    val train_df=split_data(0)
    val test_df=split_data(1)
    //(1)逻辑回归模型
    val lr=new LogisticRegression()
    //训练模型
    val lr_model=lr.fit(train_df)
    //评估模型
    val rest1=lr_model.transform(test_df)
    val accu=rest1.filter("label==prediction").count()/rest1.count().toDouble
    println("lr accuracy = " + accu)
    val metrics=new BinaryClassificationMetrics(rest1.select("prediction","label").rdd.map(v=>(v.getDouble(0),v.getInt(1))))
    val fScore = metrics.fMeasureByThreshold(0.5)
    println("lr f1 = " + fScore.collect()(0)._2)
    //（2）决策树模型
    val dt=new DecisionTreeClassifier()
    //训练模型
    val dt_model=dt.fit(train_df)
    //评估模型
    val rest2=dt_model.transform(test_df)
    val accu2=rest2.filter("label==prediction").count()/rest2.count().toDouble
    println("dt accuracy = " + accu2)
    val metrics2=new BinaryClassificationMetrics(rest2.select("prediction","label").rdd.map(v=>(v.getDouble(0),v.getInt(1))))
    val fScore2 = metrics2.fMeasureByThreshold(0.5)
    println("dt f1 = " + fScore2.collect()(0)._2)
    //根据评价指标可见，逻辑回归模型效果最优，故采用逻辑回归模型对全量数据进行预测
    var all_pre=lr_model.transform(data_set)
    //构造输出格式
    all_pre=all_pre.select("label","exchange","stock_symbol","date","stock_price_open","stock_price_high","stock_price_low","stock_volume","prediction")
     all_pre.rdd.map(v=>("label:"+v(0)+","+"stock:"+v(1)+","+v(2)+","+v(3)+","+v(4)+","+v(5)+","+v(6)+","+v(7)+","+"result:"+v(8))).repartition(1).saveAsTextFile("/Users/shencongshan/Documents/金融大数据处理技术/实验5/rest")

  }
}
