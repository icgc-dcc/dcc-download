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

import static java.util.Collections.singletonList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.common.core.model.FieldNames;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.core.StaticDownloadJob;

@Slf4j
public abstract class Task {

  public abstract void execute(TaskContext taskContext);

  protected void writeOutput(DownloadDataType dataType, TaskContext taskContext, JavaRDD<String> output) {
    val outputPath = getOutputPath(taskContext.getJobId(), taskContext.getOutputDir(), dataType);
    log.debug("Saving output to {}", outputPath);
    output.saveAsTextFile(outputPath, GzipCodec.class);
  }

  private static String getOutputPath(String jobId, String outputDir, DownloadDataType dataType) {
    return StaticDownloadJob.STATIC_DIR_PATH.equals(jobId) ?
        outputDir + "/" + dataType.getId() :
        outputDir + "/" + jobId + "/" + dataType.getId();
  }

  protected static JavaRDD<String> getHeader(JavaSparkContext sparkContext, DownloadDataType dataType) {
    val columns = dataType.getFields().entrySet().stream()
        .map(e -> e.getValue())
        .collect(toImmutableList());

    val header = Joiners.TAB.join(columns);

    return sparkContext.parallelize(singletonList(header));
  }

  protected DataFrame readInput(TaskContext taskContext, DownloadDataType dataType) {
    val sparkContext = taskContext.getSparkContext();
    val sqlContext = new SQLContext(sparkContext);
    val inputPath = taskContext.getInputDir() + "/" + dataType.getCanonicalName();
    val input = sqlContext.read().parquet(inputPath);

    return isReadAll(taskContext) ? input : filterDonors(input, taskContext.getDonorIds());
  }

  protected DataFrame filterDonors(DataFrame input, Set<String> donorIds) {
    val filterCondition = input.col(FieldNames.DONOR_ID).in(donorIds.toArray());

    return input.filter(filterCondition);
  }

  // Donor IDs should be empty only when the export static files task is executed. The download server should never pass
  // empty donor IDs set when serving client requests.
  private static boolean isReadAll(TaskContext taskContext) {
    return taskContext.getDonorIds().isEmpty();
  }

}
