import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, StreamingContext}

object spark04_foreachRdd {
  def main(args: Array[String]): Unit = {
    //创建配置
    /**
     * wordcount名字自定义
     * local[2] 表示本地模式，2个线程
     */
    val conf=new SparkConf().setAppName("wordcount").setMaster("local[2]")
    /**
     * Durations.seconds(5) 表示采集周期5秒
     */
    val ssc=new StreamingContext(conf,Durations.seconds(5))
    //从指定端口读取数据
    /**
     * hadoop102 表示Liunx主机名
     * 9999 表示端口号
     */
    val socket=ssc.socketTextStream("hadoop102",9998)
    //对数据进行操作
    /**
     * 按空格切分扁平化
     */
    val flat=socket.flatMap(_.split(" "))
    //转换结构
    val map=flat.map((_,1))
    //计数
    val reduct=map.reduceByKey(_+_)
    //把计算结果输出到文件
    //reduct.saveAsTextFiles("路径")
    //通过sparkSql对数据操作
    val spark=SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换
    import spark.implicits._
    reduct.foreachRDD(
      rdd=>{
        //将RDD转为DF,("word","count")表示DStream表中的结构，toDF表示必须是隐式转换
        val df=rdd.toDF("word","count")
        //创建临时表
        df.createOrReplaceTempView("words")
        //从临时表中查数据
        spark.sql("select * from words").show()
      }
    )
    ssc.start()
    //等待采集结束程序才结束
    ssc.awaitTermination()
  }
}
