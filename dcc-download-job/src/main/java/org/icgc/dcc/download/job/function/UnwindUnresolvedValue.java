/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.download.job.function;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.job.utils.Rows.convertToList;
import static org.icgc.dcc.download.job.utils.Tuples.tuple;

import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;

import scala.Tuple2;

@RequiredArgsConstructor
public final class UnwindUnresolvedValue
    implements
    PairFlatMapFunction<Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>, Tuple2<Map<String, String>, Map<String, Object>>, Row> {

  @NonNull
  private final String field;

  @Override
  public Iterable<Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>> call(
      Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> tuple) throws Exception {
    val key = tuple._1;
    val prevUnresolvedValues = key._2;
    val fieldValue = prevUnresolvedValues.get(field);
    checkNotNull(fieldValue, "Failed to unwind field '%s'", field);

    val resolvedValues = key._1;
    val unresolvedValues = removeValue(prevUnresolvedValues, field);
    val rows = convertToList(fieldValue);

    return rows.stream()
        .map(row -> tuple(tuple(resolvedValues, unresolvedValues), row))
        .collect(toImmutableList());
  }

  private static Map<String, Object> removeValue(Map<String, Object> unresolvedValues, String field) {
    return unresolvedValues.entrySet().stream()
        .filter(e -> !field.equals(e.getKey()))
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

}