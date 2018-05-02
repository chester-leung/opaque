/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.rise.opaque.execution

import edu.berkeley.cs.rise.opaque.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.execution.SparkPlan

case class ObliviousSortExec(order: Seq[SortOrder], child: SparkPlan)
  extends UnaryExecNode with OpaqueOperatorExec {

  override def output: Seq[Attribute] = child.output

  override def executeBlocked() = {
    val orderSer = Utils.serializeSortOrder(order, child.output)
    ObliviousSortExec.sort(child.asInstanceOf[OpaqueOperatorExec].executeBlocked(), orderSer)
  }
}

object ObliviousSortExec {
  import Utils.time

  def sort(childRDD: RDD[Block], orderSer: Array[Byte]): RDD[Block] = {
    Utils.ensureCached(childRDD)
    time("force child of ObliviousSort") { childRDD.count }
    // RA.initRA(childRDD)

    time("oblivious sort") {
      val numPartitions = childRDD.partitions.length
      val result =
        if (numPartitions <= 1) {
          childRDD.map { block =>
            val (enclave, eid) = Utils.initEnclave()
            val sortedRows = enclave.ObliviousSort(eid, orderSer, block.bytes)
            Block(sortedRows)
          }
        } else {
          ???
        }
      Utils.ensureCached(result)
      result.count()
      result
    }
  }
}
