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
import lombok.val;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.function.PairByProject;
import org.icgc.dcc.download.job.utils.Tasks;

public class ProjectStaticTask extends Task {

  @Override
  public void execute(TaskContext taskContext) {
    val dataTypes = taskContext.getDataTypes();
    checkState(dataTypes.size() == 1, "Unexpeceted datatypes {}", dataTypes);
    val dataType = dataTypes.iterator().next();

    val inputInput = readInput(taskContext, dataType).javaRDD();
    val delegate = Tasks.createTask(dataType);
    val records = delegate.process(inputInput, dataType);

    val projectIdIndex = resolveProjectIdIndex(dataType);
    records.mapToPair(new PairByProject(projectIdIndex));
    // TODO: Now save as here:
    // http://stackoverflow.com/questions/23995040/write-to-multiple-outputs-by-key-spark-one-spark-job

  }

  private static int resolveProjectIdIndex(DownloadDataType dataType) {
    val index = dataType.getDownloadFields().indexOf("_project_id");
    checkState(index != -1, "Failed to resolve project ID index of '%s' data type.", dataType);

    return index;
  }

}
