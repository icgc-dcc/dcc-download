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

import static com.google.common.base.Preconditions.checkState;
import lombok.NoArgsConstructor;
import lombok.val;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.function.ConvertRow;

@NoArgsConstructor
public class GenericTask extends Task {

  @Override
  public void execute(TaskContext taskContext) {
    val dataTypes = taskContext.getDataTypes();
    checkState(dataTypes.size() == 1, "Unexpeceted datatypes {}", dataTypes);
    val dataType = dataTypes.iterator().next();

    val input = readInput(taskContext, dataType);
    val filteredInput = filterDonors(input, taskContext.getDonorIds())
        .javaRDD();

    val records = process(filteredInput, dataType);

    val header = getHeader(taskContext.getSparkContext(), dataType);
    val output = header.union(records);

    writeOutput(dataType, taskContext, output);
  }

  protected JavaRDD<String> process(JavaRDD<Row> input, DownloadDataType dataType) {
    return input.map(new ConvertRow(dataType.getDownloadFileds()));
  }

  private DataFrame readInput(TaskContext taskContext, DownloadDataType dataType) {
    val sparkContext = taskContext.getSparkContext();
    val sqlContext = new SQLContext(sparkContext);
    val inputPath = taskContext.getInputDir() + "/" + dataType.getCanonicalName();

    return sqlContext.read().parquet(inputPath);
  }

}
