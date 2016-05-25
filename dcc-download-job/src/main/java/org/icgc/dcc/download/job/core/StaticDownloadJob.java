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
package org.icgc.dcc.download.job.core;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import java.util.Map;
import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.task.ClinicalTask;
import org.icgc.dcc.download.job.task.ProjectStaticTask;
import org.icgc.dcc.download.job.task.Task;
import org.icgc.dcc.download.job.task.TaskContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

@Slf4j
public class StaticDownloadJob implements DownloadJob {

  public static final String STATIC_DIR_PATH = "static";

  @Override
  public void execute(JobContext jobContext) {
    log.info("Starting generate static download files job...");
    createTasks(jobContext).entrySet().parallelStream()
        .forEach(e -> {
          Task task = e.getKey();
          TaskContext context = e.getValue();
          context.getSparkContext().setJobGroup("static-download", "static-download");
          task.execute(context);
        });
    log.info("Finished generate static download files job.");
  }

  private static Map<? extends Task, TaskContext> createTasks(JobContext jobContext) {
    val tasks = ImmutableMap.<Task, TaskContext> builder();
    tasks.put(new ClinicalTask(), createTaskContext(jobContext, DownloadDataType.CLINICAL));

    for (val dataType : getNonClinicalDataTypes()) {
      tasks.put(new ProjectStaticTask(), createTaskContext(jobContext, singleton(dataType)));
    }

    return tasks.build();
  }

  private static Iterable<DownloadDataType> getNonClinicalDataTypes() {
    val dataTypes = Sets.newHashSet(DownloadDataType.values());
    dataTypes.removeAll(DownloadDataType.CLINICAL);

    return dataTypes;
  }

  private static TaskContext createTaskContext(JobContext jobContext, Set<DownloadDataType> dataTypes) {
    return TaskContext.builder()
        .jobId(STATIC_DIR_PATH)
        .inputDir(jobContext.getInputDir())
        .outputDir(resolveOutputDir(jobContext.getOutputDir(), dataTypes))
        .sparkContext(jobContext.getSparkContext())
        .donorIds(emptySet())
        .dataTypes(dataTypes)
        .build();
  }

  private static String resolveOutputDir(String outputDir, Set<DownloadDataType> dataTypes) {
    val staticDir = outputDir + "/" + STATIC_DIR_PATH;

    return dataTypes.equals(DownloadDataType.CLINICAL) ?
        staticDir + "/Summary" :
        staticDir + "/Projects";
  }

}
