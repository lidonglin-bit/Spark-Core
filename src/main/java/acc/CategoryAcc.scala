package acc

import bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

//in:UserVisitAction  out:Map[(品类,"click")->count] (品类,"order") ->    (品类,"pay"),->count
class CategoryAcc extends AccumulatorV2[UserVisitAction,Map[(String,String),Long]]{

  private var map = Map[(String, String), Long]()

  // 判断累加器是否为"零"
  override def isZero: Boolean = map.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    val acc = new CategoryAcc
    acc.map = map
    acc
  }

  override def reset(): Unit = Map[(String, String), Long]()  // 不可变集合需要赋值个新的空集合

  //分区器累加
  override def add(v: UserVisitAction): Unit = {
    //分别计算3个指标
    // 对不同的行为做不同的处理  if语句 或 模式匹配
    v match {
      //点击行为
      case action if action.click_category_id != -1 =>
        val key = (action.click_category_id.toString, "click")
        // 这里其实是等价于 map = map + (.....)  不可变集合是给map赋值新的集合
        map += key -> (map.getOrElse(key, 0L) + 1L)


      // 下单行为  切出来的是字符串 "null", 不是空的null
      case action if action.order_category_ids != "null" =>
        // 切出来这次下单的多个品类
        val cIds: Array[String] = action.order_category_ids.split(",")
        cIds.foreach(cid => {
          val key: (String, String) = (cid, "order")
          map += key -> (map.getOrElse(key, 0L) + 1L)
        })

      // 支付行为
      case action if action.pay_category_ids != "null" =>
        val cIds: Array[String] = action.pay_category_ids.split(",")
        cIds.foreach(cid => {
          val key: (String, String) = (cid, "pay")
          map += key -> (map.getOrElse(key, 0L) + 1L)
        })
      // 其他非正常情况, 做任何处理
      case _ =>
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    // 1.把other中的map合并到map中
    // 合并map
   /*other match {
      case o: CategoryAcc =>
        o.map.foreach {
          case (key, count) =>
            map += key -> (map.getOrElse(key, 0L) + count)
        }
      case _ =>
        throw new UnsupportedOperationException
    }*/
    // 2.对other的map进行折叠, 把结果都折叠到map中
    // 如果是可变map, 则所有的变化都是在原集合中发生变化, 最后的值可以不用再一次添加
    // 如果是不变map, 则计算的结果, 必须重新赋值给原的map变量
    map = other match {
      case o: CategoryAcc =>
        o.map.foldLeft(map) {
          // case出来的任何东西都不能改, 只能读
          case (map, (cidAction, count)) =>
            // 对不可变来说所以这是错的. !!!!!!!!!!!
            // map += cidAction -> (map.getOrElse(cidAction, 0L) + count)
            // 相当于 map = map + (....)
            //直接返回新的集合就可以了
            map + (cidAction -> (map.getOrElse(cidAction, 0L) + count))
        }
      case _ =>
        throw new UnsupportedOperationException
    }
    }


  override def value: Map[(String, String), Long] = map
}
