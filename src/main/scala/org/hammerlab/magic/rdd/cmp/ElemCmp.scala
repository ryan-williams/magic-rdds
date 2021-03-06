package org.hammerlab.magic.rdd.cmp

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Given an outer join of two [[RDD]]s with the presence or absence of values for a key replaced with a [[Boolean]] for
 * each [[RDD]], expose statistics about how many elements exist in either or both [[RDD]]s.
 */
class ElemCmp[T: ClassTag] private(joined: RDD[(T, (Boolean, Boolean))]) {
  lazy val stats =
    (for {
      (e, (a, b)) <- joined
    } yield
      (a, b) match {
        case (true, true) => ElemCmpStats(both = 1)
        case (true, false) => ElemCmpStats(onlyA = 1)
        case (false, true) => ElemCmpStats(onlyB = 1)
        case (false, false) => throw new Exception(s"Invalid entry: $e")
      }
    ).reduce(_ + _)

  lazy val ElemCmpStats(eq, oa, ob) = stats
  lazy val isEqual = stats.isEqual

  lazy val bothRDD =
    for {
      (e, (a, b)) <- joined
      if a && b
    } yield
      e

  lazy val both = bothRDD.collect()
  def both(num: Int = 10000) = bothRDD.take(num)

  lazy val aRDD =
    for {
      (e, (a, b)) <- joined
      if a && !b
    } yield
      e

  lazy val a = aRDD.collect()
  def a(num: Int = 10000) = aRDD.take(num)

  lazy val bRDD =
    for {
      (e, (a, b)) <- joined
      if !a && b
    } yield
      e

  lazy val b = bRDD.collect()
  def b(num: Int = 10000) = bRDD.take(num)
}

object ElemCmp {
  def apply[T: ClassTag](rdd1: RDD[T], rdd2: RDD[T]): ElemCmp[T] =
    new ElemCmp(
      for {
        (e, (aO, bO)) <- rdd1.map(_ -> null).fullOuterJoin(rdd2.map(_ -> null))
      } yield {
        e -> (aO.isDefined, bO.isDefined)
      }
    )
}
