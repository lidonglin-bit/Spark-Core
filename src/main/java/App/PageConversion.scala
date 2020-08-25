package App

import java.text.DecimalFormat

import bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageConversion {
  def statPageConversionRate(sc:SparkContext,
                             UserVisitActionRDD: RDD[UserVisitAction],
                             pageString:String): Unit ={

    //1.做出来目标跳转流
    val pages = pageString.split(",")
    val prePages = pages.take(pages.length-1)
    val postPages = pages.takeRight(pages.length-1)
    //结果为List(1->2, 2->3, 3->4, 4->5, 5->6, 6->7)
    val targetPageFlows = prePages.zip(postPages).map {
      case (pre, post) => s"$pre->$post"
    }
    //1.1把targetpages做广播变量，优化性能
    val targetPageFlowBC = sc.broadcast(targetPageFlows)
    //2.计算分母，计算需要页面的点击量 Map(5 -> 3563, 1 -> 3640, 6 -> 3593, 2 -> 3559, 3 -> 3672, 4 -> 3602)
    val pageAndCount = UserVisitActionRDD.filter(action => {
      prePages.contains(action.page_id.toString)
    })
      .map(action => (action.page_id, 1))
      .countByKey()

    //3.计算分子
    //3.1 按照sessionId分组,不能先对需要的页面做过滤，否则会应用调整的逻辑
    val sessionGrouped: RDD[(String, Iterable[UserVisitAction])] = UserVisitActionRDD.groupBy(_.session_id)
    var pageFlowsRDD = sessionGrouped.flatMap {
      case (sid, actionit) =>
        //把每个session的行为做一个时间排序
        val actions: List[UserVisitAction] = actionit.toList.sortBy(_.action_time)
        val preActions = actions.take(actions.length - 1)
        val postActions = actions.takeRight(actions.length - 1)

        preActions.zip(postActions).map {
          case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
        }.filter(flow => targetPageFlowBC.value.contains(flow)) //使用广播变量
    }

    //3.2聚合
    val pageFlowAndCount: collection.Map[String, Long] = pageFlowsRDD.map((_, 1)).countByKey()

    val f = new DecimalFormat(".00%")
    //4.计算跳转率
    val result: collection.Map[String, Any] = pageFlowAndCount.map {
          //pageAndCount分母
          //1->2  count/1的点击量
          case (flow, count) =>
            val rate = count.toDouble / pageAndCount(flow.split("->")(0).toLong)
            (flow,f.format(rate).toString)
        }
    println(result)

  }

}
/*
1,2,4,5,4,7  计算他们的跳转率
1.想办法做出来跳转流
      1->2,  2->3   3->4
2.计算跳转率
     1->2跳转率
     分子
         1->2跳转流的个数
            如何计算？
                1.保证是同一个session才能计算，其实就是按照session进行分组

                2.按照时间排序

                3.RDD["1->2","1->2"."2->3"]  map() reduceByKey
                  RDD[UserVisitAction]  map
                  RDD[1,2,3,4,5,6,7]
                  如果做跳转流
                  rdd1= RDD[1,2,3,4,5,6]
                  rdd2= RDD[2,3,4,5,6,7]
                  rdd3 = rdd1.zip(zip).map(...)
                  过滤出来目标跳转流，然后再聚合


     分母
        页面：1.的点击数
 */