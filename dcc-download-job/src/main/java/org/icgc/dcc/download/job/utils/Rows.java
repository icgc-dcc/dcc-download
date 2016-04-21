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
package org.icgc.dcc.download.job.utils;

import static java.util.Arrays.stream;
import static lombok.AccessLevel.PRIVATE;
import static scala.collection.JavaConversions.asJavaList;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.sql.Row;
import org.icgc.dcc.common.core.util.Separators;

import scala.collection.mutable.WrappedArray;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Rows {

  public static String getValue(@NonNull Row row, @NonNull Map<String, String> resolvedValues, @NonNull String field) {
    val providedValue = resolvedValues.get(field);
    if (providedValue != null) {
      return providedValue;
    }

    return getValue(row, field);
  }

  public static Object getObjectValue(@NonNull Row row, @NonNull String field) {
    Object value = null;
    try {
      val fieldIndex = row.fieldIndex(field);
      value = row.get(fieldIndex);
    } catch (IllegalArgumentException e) {
      log.warn("Field {} doesn't exist in row {}", field, row);
    }

    return value;
  }

  public static String getValue(@NonNull Row row, @NonNull String field) {
    val value = getObjectValue(row, field);

    return value == null ? Separators.EMPTY_STRING : String.valueOf(value);
  }

  public static boolean hasField(@NonNull Row row, @NonNull String fieldName) {
    val schema = row.schema();
    schema.fieldNames();
    return stream(schema.fieldNames())
        .anyMatch(f -> f.equals(fieldName));
  }

  public static List<Row> getValueAsList(Row source, String field) {
    log.debug("Getting value of '{}' from row: {}", field, source);
    val values = source.getAs(field);

    return values == null ? Collections.emptyList() : convertToList(values);
  }

  public static List<Row> convertToList(@NonNull Object rawValue) {
    @SuppressWarnings("unchecked")
    val values = (WrappedArray<Row>) rawValue;

    return asJavaList(values);
  }

  public static boolean canBeResolved(Row row, String field) {
    val value = row.getAs(field);

    // Spark reads Parquet collections in WrappedArray<Row>
    return !(value instanceof WrappedArray<?>);
  }

}
