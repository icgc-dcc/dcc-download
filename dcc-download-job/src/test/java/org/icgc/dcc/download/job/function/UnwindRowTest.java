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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.job.util.TestRows.createDonorSchema;
import static org.icgc.dcc.download.job.util.TestRows.createRow;
import static org.icgc.dcc.download.job.util.TestRows.createRows;
import static org.icgc.dcc.download.job.util.TestRows.createSampleSchema;
import static org.icgc.dcc.download.job.util.TestRows.createSpecimenSchema;
import lombok.val;

import org.apache.spark.sql.Row;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.download.job.util.TestRows;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class UnwindRowTest {

  UnwindRow function;

  @Test
  public void testCall_firstLevel() throws Exception {
    function = new UnwindRow(ImmutableList.of(FieldNames.DONOR_SPECIMEN));
    val result = function.call(createDonorRow());

    assertThat(result).hasSize(1);
    val row = Iterables.getFirst(result, null);
    Object specimenId = row.getAs(FieldNames.DONOR_SPECIMEN_ID);
    assertThat(specimenId).isEqualTo("specimen_1");
  }

  @Test
  public void testCall_secondLevel() throws Exception {
    function = new UnwindRow(ImmutableList.of(FieldNames.DONOR_SPECIMEN, FieldNames.DONOR_SAMPLE));
    val result = function.call(createDonorRow());

    assertThat(result).hasSize(2);
    assertRow(Iterables.get(result, 0), FieldNames.DONOR_SAMPLE_ID, "sample_id_1");
    assertRow(Iterables.get(result, 1), FieldNames.DONOR_SAMPLE_ID, "sample_id_2");
  }

  @Test
  public void testCall_missingElement() throws Exception {
    function = new UnwindRow(ImmutableList.of(FieldNames.DONOR_SAMPLE));
    val sampleRow = TestRows.createRow(createSpecimenSchema(), "specimen_1", null);

    val result = function.call(sampleRow);
    assertThat(result).isEmpty();
  }

  private static void assertRow(Row row, String fieldName, Object expectedValue) {
    Object rowValue = row.getAs(fieldName);
    assertThat(rowValue).isEqualTo(expectedValue);
  }

  private static Row createDonorRow() {
    val sampleSchema = createSampleSchema();
    val samples = createRows(createRow(sampleSchema, "sample_id_1"), createRow(sampleSchema, "sample_id_2"));

    val specimenSchema = createSpecimenSchema();
    val specimens = createRows(createRow(specimenSchema, "specimen_1", samples));

    val donorSchema = createDonorSchema();
    val donor = createRow(donorSchema, "DO1", specimens);

    return donor;
  }

}
