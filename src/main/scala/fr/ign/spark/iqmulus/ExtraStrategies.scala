/*
 * Copyright 2015 IGN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.ign.spark.iqmulus

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.InternalRow 
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, Strategy => SQLStrategy, SQLContext}
import org.apache.spark.sql.catalyst.expressions.{Alias, IntegerLiteral}
//import org.apache.spark.sql.execution.datasources.LogicalRelation // private, see workaround below...
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.{Min, Max, Count, Expression, NamedExpression}

// optimized counts for PLY and LAS relations
case class CountPlan(n: Long, sections: Array[BinarySection], name : String) extends SparkPlan {
  println("CountPlan optimization !")
  def count = n*sections.map(_.count).sum
  
  override def executeCollect(): Array[Row] = Array(Row(count))
  
  def doExecute() = sqlContext.sparkContext.parallelize(Seq(InternalRow(count)),1)

  def output: Seq[AttributeReference] = Seq(AttributeReference(name,LongType,nullable=false)())

  def children: Seq[SparkPlan] = Nil
}

// optimized aggregations for LAS relations (x/y/z)(min/xmax) and count
case class AggregatePlan(aggregateExpressions: Seq[NamedExpression], headers: Array[las.LasHeader]) extends SparkPlan {
  println("AggregatePlan optimization !")
  
  def get(expression: Expression) : Any = expression match {
    case Alias(expr : Expression, _)        => get(expr)
    case Min(AttributeReference("x",_,_,_)) => headers.map(h => (h.pmin(0)-h.offset(0))/h.scale(0)).min.toInt
    case Max(AttributeReference("x",_,_,_)) => headers.map(h => (h.pmax(0)-h.offset(0))/h.scale(0)).max.toInt
    case Min(AttributeReference("y",_,_,_)) => headers.map(h => (h.pmin(1)-h.offset(1))/h.scale(1)).min.toInt
    case Max(AttributeReference("y",_,_,_)) => headers.map(h => (h.pmax(1)-h.offset(1))/h.scale(1)).max.toInt
    case Min(AttributeReference("z",_,_,_)) => headers.map(h => (h.pmin(2)-h.offset(2))/h.scale(2)).min.toInt
    case Max(AttributeReference("z",_,_,_)) => headers.map(h => (h.pmax(2)-h.offset(2))/h.scale(2)).max.toInt
    case Count(IntegerLiteral(n))           => headers.map(_.pdr_nb ).sum * n
  }
  
  override def executeCollect(): Array[Row] = Array(Row.fromSeq(aggregateExpressions.map(get _)))
  
  def doExecute() = sqlContext.sparkContext.parallelize(Seq(InternalRow.fromSeq(aggregateExpressions.map(get _))),1)

  def output: Seq[AttributeReference] = aggregateExpressions.map(a => AttributeReference(a.toAttribute.name,a.toAttribute.dataType)())

  def children: Seq[SparkPlan] = Nil
}

object AggregatePlan {  
  def handles(expression: Expression): Boolean = expression match {
    case Alias(expr : Expression, _)    => AggregatePlan handles expr
    case Min(attr : AttributeReference) => Seq("x","y","z") contains attr.name
    case Max(attr : AttributeReference) => Seq("x","y","z") contains attr.name
    case Count(_)                       => true
    case _ => false
  }
}

object Strategy extends SQLStrategy with Serializable {
  
  //work around private[sql] for org.apache.spark.sql.execution.datasources.LogicalRelation
  def relationMap[Relation](plan : LogicalPlan, f : Relation => Seq[SparkPlan]) = {
      val field = plan.getClass.getDeclaredFields.find(_.getName == "relation")
      field.foreach(_.setAccessible(true))
      field.map(_.get(plan)) match {
        case Some(relation : Relation) => f(relation)
        case _ => Nil
      }
  }
  
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      
    case Aggregate(Nil,Seq(Alias(Count(IntegerLiteral(n)),name)),Project(_,logicalRelation)) =>
      relationMap(logicalRelation,{ relation : BinarySectionRelation =>
          new CountPlan(n,relation.sections,name) :: Nil
      })

    case Aggregate(Nil,agg,Project(_,logicalRelation)) =>
      relationMap(logicalRelation,{ relation : las.LasRelation =>
          if (!(agg forall (AggregatePlan handles _)))    Nil
          else new AggregatePlan(agg,relation.headers) :: Nil
      })

      // debug
    case Aggregate(exprs,aggs,child) =>
      println("--> Unmatched Aggregate !")
      println(exprs.getClass)  
      println(exprs)  
      println(aggs.getClass)  
      println(aggs)  
      println(child.getClass)  
      println(child) 
      Nil
      
      // debug
    case _ => 
      println("--> Unmatched!")
      println(plan.getClass)  
      println(plan)  
      Nil
  }

  def register(bool : Boolean = true)(implicit sqlContext : SQLContext) = {
    val strategies = sqlContext.experimental.extraStrategies.diff(Seq(Strategy))
    sqlContext.experimental.extraStrategies = if (bool) Strategy +: strategies else strategies
  }
}

