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
package org.icgc.dcc.download.job.task;

import static org.icgc.dcc.common.core.model.FieldNames.DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SAMPLE;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN;
import static org.icgc.dcc.common.core.model.FieldNames.DONOR_SPECIMEN_ID;
import static org.icgc.dcc.common.core.model.FieldNames.PROJECT_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_DONOR_ID;
import static org.icgc.dcc.common.core.model.FieldNames.SubmissionFieldNames.SUBMISSION_SPECIMEN_ID;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.Separators.UNDERSCORE;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.function.AddFieldsToKey;
import org.icgc.dcc.download.job.function.ConvertRow;
import org.icgc.dcc.download.job.function.ConvertRecord;
import org.icgc.dcc.download.job.function.PairByFields;
import org.icgc.dcc.download.job.function.UnwindRow;

import com.google.common.collect.ImmutableList;

public class ClinicalTask extends Task {

  /**
   * 
   */
  private static final ImmutableList<String> DONOR_FIELDS = ImmutableList.of(DONOR_ID, PROJECT_ID, SUBMISSION_DONOR_ID);

  @Override
  public void execute(TaskContext taskContext) {
    val input = readInput(taskContext);
    val donors = filterDonors(input, taskContext.getDonorIds())
        .javaRDD();

    donors.cache();

    val dataTypes = taskContext.getDataTypes();
    if (dataTypes.contains(DownloadDataType.DONOR)) {
      writeDonors(taskContext, donors);
    }

    if (dataTypes.contains(DownloadDataType.DONOR_EXPOSURE)) {
      writeDonorNestedType(taskContext, donors, DownloadDataType.DONOR_EXPOSURE);
    }

    if (dataTypes.contains(DownloadDataType.DONOR_FAMILY)) {
      writeDonorNestedType(taskContext, donors, DownloadDataType.DONOR_FAMILY);
    }

    if (dataTypes.contains(DownloadDataType.DONOR_THERAPY)) {
      writeDonorNestedType(taskContext, donors, DownloadDataType.DONOR_THERAPY);
    }

    if (dataTypes.contains(DownloadDataType.SPECIMEN)) {
      writeDonorNestedType(taskContext, donors, DownloadDataType.SPECIMEN);
    }

    if (dataTypes.contains(DownloadDataType.SAMPLE)) {
      writeSample(taskContext, donors);
    }

    donors.unpersist(false);
  }

  private void writeDonors(TaskContext taskContext, JavaRDD<Row> donors) {
    val dataType = DownloadDataType.DONOR;
    val header = getHeader(taskContext.getSparkContext(), dataType);
    val records = donors.map(new ConvertRow(dataType.getDownloadFileds()));
    val output = header.union(records);

    writeOutput(dataType, taskContext, output);
  }

  private void writeDonorNestedType(TaskContext taskContext, JavaRDD<Row> donors, DownloadDataType dataType) {
    val unwindPath = resolveDonorNestedPath(dataType);

    val records = donors.mapToPair(new PairByFields(DONOR_FIELDS))
        .flatMapValues(new UnwindRow(ImmutableList.of(unwindPath)))
        .map(new ConvertRecord(dataType.getDownloadFileds()));

    val header = getHeader(taskContext.getSparkContext(), dataType);
    val output = header.union(records);

    writeOutput(dataType, taskContext, output);
  }

  private void writeSample(TaskContext taskContext, JavaRDD<Row> donors) {
    val donorFields = DONOR_FIELDS;
    val specimenFields = ImmutableList.of(DONOR_SPECIMEN_ID, SUBMISSION_SPECIMEN_ID);

    val dataType = DownloadDataType.SAMPLE;

    val records = donors.mapToPair(new PairByFields(donorFields))
        .flatMapValues(new UnwindRow(ImmutableList.of(DONOR_SPECIMEN)))
        .mapToPair(new AddFieldsToKey(specimenFields))
        .flatMapValues(new UnwindRow(ImmutableList.of(DONOR_SAMPLE)))
        .map(new ConvertRecord(dataType.getDownloadFileds()));

    val header = getHeader(taskContext.getSparkContext(), dataType);
    val output = header.union(records);

    writeOutput(dataType, taskContext, output);
  }

  private static String resolveDonorNestedPath(DownloadDataType dataType) {
    val nestedName = dataType.getId();
    val donorName = DownloadDataType.DONOR.getId();

    return nestedName.replace(donorName + UNDERSCORE, EMPTY_STRING);
  }

  private static DataFrame readInput(TaskContext taskContext) {
    val sparkContext = taskContext.getSparkContext();
    val sqlContext = new SQLContext(sparkContext);
    val inputPath = taskContext.getInputDir() + "/" + DownloadDataType.DONOR.getId();

    return sqlContext.read().parquet(inputPath);
  }

}
