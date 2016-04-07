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

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.job.utils.Rows.hasField;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.utils.Rows;

public final class ConvertGenericType implements FlatMapFunction<Row, String> {

  private final ConvertRow converter;
  private final List<String> downloadFields;

  /**
   * Represents a schema of this {@link DownloadDataType}, where the value has following meaning:<br>
   * <ul>
   * <li>{@code true} - look for the value in the row</li>
   * <li>{@code false} - look for the value in the consequence</li>
   * </ul>
   */
  private transient Map<String, Boolean> typeSchema;

  public ConvertGenericType(@NonNull DownloadDataType dataType) {
    this.converter = new ConvertRow(dataType);
    this.downloadFields = dataType.getDownloadFileds();
  }

  @Override
  public Iterable<String> call(Row row) throws Exception {
    return hasConsequences(row) ? convertWithConsequences(row) : Collections.singleton(converter.convert(row));
  }

  private Iterable<String> convertWithConsequences(Row row) {
    ensureSchema(row);
    val consequences = Rows.getValueAsList(row, FieldNames.OBSERVATION_CONSEQUENCES);

    return consequences.stream()
        .map(cons -> convertConsequence(row, cons))
        .collect(toImmutableList());
  }

  private String convertConsequence(Row row, Row consequence) {
    val values = downloadFields.stream()
        .map(field -> getValue(consequence, row, field))
        .collect(toImmutableList());

    return Joiners.TAB.join(values);
  }

  private String getValue(Row consequence, Row row, String field) {
    val getFromRow = typeSchema.get(field);

    return getFromRow ? Rows.getValue(row, field) : Rows.getValue(consequence, field);
  }

  private void ensureSchema(Row row) {
    if (typeSchema == null) {
      typeSchema = downloadFields.stream()
          .collect(toImmutableMap(f -> f, resolveFieldLocation(row)));
    }
  }

  private static Function<String, Boolean> resolveFieldLocation(Row row) {
    return f -> {
      return hasField(row, f);
    };
  }

  private boolean hasConsequences(Row row) {
    return hasField(row, FieldNames.OBSERVATION_CONSEQUENCES);
  }

}