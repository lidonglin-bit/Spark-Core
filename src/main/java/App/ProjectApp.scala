package App

import bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("project")
    val sc = new SparkContext(conf)

    //从数据把文件读出
    val sourceRDD = sc.textFile("D:\\idea\\spark-knight1\\input\\user_visit_action.txt")

    //把数据封装号(封装到样例类中)
    val userVisitActionRDD: RDD[UserVisitAction] = sourceRDD.map(line => {
      val fields = line.split("_")
      UserVisitAction(
        fields(0),
        fields(1).toLong,
        fields(2),
        fields(3).toLong,
        fields(4),
        fields(5),
        fields(6).toLong,
        fields(7).toLong,
        fields(8),
        fields(9),
        fields(10),
        fields(11),
        fields(12).toLong)
    })

    //需求1：
    //val categoryTop10 = CategoryTopApp.calcCategoryTop10(sc, userVisitActionRDD)
    //需求2：top10piping类的top10session
    //CtegorySessionTopApp.statCategorySessionTop10_4(sc,categoryTop10,userVisitActionRDD)
    //需求3
    PageConversion.statPageConversionRate(sc,userVisitActionRDD,"1,2,3,4,5,6,7")
    //关闭项目(sc)
    sc.stop()
  }

}
