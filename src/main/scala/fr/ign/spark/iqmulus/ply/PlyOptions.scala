/*
 * Copyright 2015-2019 IGN
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

package fr.ign.spark.iqmulus.ply

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import fr.ign.spark.iqmulus.HeaderOptions

/**
 * Options for the Ply data source.
 */
private[ply] class PlyOptions(@transient protected val parameters: CaseInsensitiveMap[String]) {

  import PlyOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * element to use.
   */
  val element = parameters.getOrElse(ELEMENT, VERTEX)

  /**
   * littleEndian
   */
  val littleEndian = parameters.getOrElse(LITTLE_ENDIAN, TRUE).toBoolean
}

private[ply] object PlyOptions {
  val ELEMENT = "element"
  val VERTEX = "vertex"
  val LITTLE_ENDIAN = "littleEndian"
  val TRUE = "true"
}
