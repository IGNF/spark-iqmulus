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
import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.{Alias, IntegerLiteral}
//import org.apache.spark.sql.execution.datasources.LogicalRelation // private, see workaround below...
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.catalyst.expressions.AttributeReference

case class CountPlan(n: Long, sections: Array[BinarySection], name : String) extends SparkPlan {
	
  def count = n*sections.map(_.count).sum
  
  override def executeCollect(): Array[Row] = Array(Row(count))
	
  def doExecute() = sqlContext.sparkContext.parallelize(Seq(InternalRow(count)),1)

  def output: Seq[AttributeReference] = Seq(AttributeReference(name,LongType,nullable=false)())

  def children: Seq[SparkPlan] = Nil
}

object Strategy extends SQLStrategy with Serializable {
	def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
		// Find direct counts.
	  case Aggregate(List(),List(Alias(Count(IntegerLiteral(lit)),name)),Project(_,child)) =>
  	  //workaround private[sql] for org.apache.spark.sql.execution.datasources.LogicalRelation
	    val field = child.getClass.getDeclaredFields.find(_.getName == "relation")
	    field.foreach(_.setAccessible(true))
	    field.map(_.get(child)) match {
  	      case Some(r : BinarySectionRelation) => CountPlan(lit,r.sections,name) :: Nil
	      case _ => Nil
	    }
	  case Aggregate(List(),List(Alias(Count(IntegerLiteral(lit)),name)),child) =>
	    println("Filtered Count optimization ???")  
	    println(child.getClass)  
	    println(child)  
        Nil
	    
	  case _ => 
	    println(plan.getClass)  
	    println(plan)  
	    Nil // Return an empty list if we don't know how to handle this plan.
	}

	def register(bool : Boolean, sqlContext : SQLContext) = {
		val strategies = sqlContext.experimental.extraStrategies.diff(Seq(Strategy))
		sqlContext.experimental.extraStrategies = if (bool) Strategy +: strategies else strategies
	}
}

