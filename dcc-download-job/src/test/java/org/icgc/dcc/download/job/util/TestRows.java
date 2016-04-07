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
package org.icgc.dcc.download.job.util;

import static lombok.AccessLevel.PRIVATE;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createArrayType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ANALYZED_SAMPLE_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE_ID;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.icgc.dcc.common.core.model.FieldNames;

import scala.collection.mutable.WrappedArray;
import scala.collection.mutable.WrappedArrayBuilder;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@NoArgsConstructor(access = PRIVATE)
public final class TestRows {

  public static Row createRow(StructType schema, Object... values) {
    return new GenericRowWithSchema(values, schema);
  }

  public static StructType createDonorSchema() {
    val fields = ImmutableList.of(
        createStructField(FieldNames.DONOR_ID, StringType, true),
        createStructField(FieldNames.DONOR_SPECIMEN, createArrayType(createSpecimenSchema()), true)
        );

    return DataTypes.createStructType(fields);
  }

  public static StructType createSpecimenSchema() {
    val fields = ImmutableList.of(
        createStructField(FieldNames.DONOR_SPECIMEN_ID, StringType, true),
        createStructField(FieldNames.DONOR_SAMPLE, createArrayType(createSampleSchema()), true)
        );

    return DataTypes.createStructType(fields);
  }

  public static StructType createSampleSchema() {
    val fields = ImmutableList.of(
        createStructField(DONOR_SAMPLE_ID, StringType, true),
        createStructField(DONOR_SAMPLE_ANALYZED_SAMPLE_ID, StringType, true)
        );
    return DataTypes.createStructType(fields);
  }

  public static StructType createExposureSchema() {
    val fields = ImmutableList.of(
        createStructField("alcohol_history", StringType, true),
        createStructField("alcohol_history_intensity", StringType, true),
        createStructField("exposure_intensity", IntegerType, true),
        createStructField("exposure_notes", StringType, true),
        createStructField("exposure_type", StringType, true),
        createStructField("tobacco_smoking_history_indicator", StringType, true),
        createStructField("tobacco_smoking_intensity", IntegerType, true)
        );
    return DataTypes.createStructType(fields);
  }

  public static WrappedArray<Row> createRows(Row... rows) {
    ClassTag<Row> tag = ClassTag$.MODULE$.apply(Row.class);
    WrappedArrayBuilder<Row> builder = new WrappedArrayBuilder<Row>(tag);

    for (val row : Lists.newArrayList(rows)) {
      builder = builder.$plus$eq(row);
    }

    return builder.result();
  }

}
