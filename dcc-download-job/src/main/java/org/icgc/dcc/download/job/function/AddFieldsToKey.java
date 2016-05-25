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

import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.download.job.utils.Rows.isRowsContainer;
import static org.icgc.dcc.download.job.utils.Rows.getObjectValue;
import static org.icgc.dcc.download.job.utils.Rows.getValue;
import static org.icgc.dcc.download.job.utils.Tuples.tuple;

import java.util.Collection;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.icgc.dcc.download.job.utils.Rows;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;

@RequiredArgsConstructor
public final class AddFieldsToKey
    implements
    PairFunction<Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>, Tuple2<Map<String, String>, Map<String, Object>>, Row> {

  @NonNull
  private final Collection<String> fields;

  @Override
  public Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> call(
      Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> tuple) {
    val row = tuple._2;
    val prevKey = tuple._1;
    val resolvedValues = ImmutableMap.<String, String> builder();
    resolvedValues.putAll(prevKey._1);
    val unresolvedValues = ImmutableMap.<String, Object> builder();
    unresolvedValues.putAll(prevKey._2);

    for (val field : fields) {
      if (!Rows.hasField(row, field)) {
        resolvedValues.put(field, EMPTY_STRING);
      }
      else if (isRowsContainer(row, field)) {
        resolvedValues.put(field, getValue(row, field));
      } else {
        unresolvedValues.put(field, getObjectValue(row, field));
      }
    }

    val key = new Tuple2<Map<String, String>, Map<String, Object>>(resolvedValues.build(), unresolvedValues.build());

    return tuple(key, row);
  }

}