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

import static java.util.Collections.singleton;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.core.model.DownloadDataType.CLINICAL;
import static org.icgc.dcc.download.core.model.DownloadDataType.hasClinicalDataTypes;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.common.core.model.Marking;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.util.DownloadJobs;
import org.icgc.dcc.download.job.task.ClinicalTask;
import org.icgc.dcc.download.job.task.GenericTask;
import org.icgc.dcc.download.job.task.SecondaryTask;
import org.icgc.dcc.download.job.task.SsmTask;
import org.icgc.dcc.download.job.task.Task;
import org.icgc.dcc.download.job.task.TaskContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@Slf4j
public class DefaultDownloadJob implements DownloadJob {

  @Override
  public void execute(JobContext jobContext) {
    log.info("Running spark job...");
    createTasks(jobContext).entrySet().parallelStream()
        .forEach(e -> {
          Task task = e.getKey();
          TaskContext context = e.getValue();
          setJobName(context);
          task.execute(context);
        });
  }

  private static void setJobName(TaskContext taskContext) {
    val jobId = taskContext.getJobId();
    val dataTypes = taskContext.getDataTypes();
    val dataType = hasClinicalDataTypes(dataTypes) ? DownloadDataType.DONOR : Iterables.get(dataTypes, 0);
    val jobName = DownloadJobs.getJobName(jobId, dataType);

    setJobGroupName(taskContext.getSparkContext(), jobName);
  }

  private static void setJobGroupName(JavaSparkContext sparkContext, String jobId) {
    val desc = "Download Job " + jobId;
    sparkContext.setJobGroup(jobId, desc);
  }

  private static Map<? extends Task, TaskContext> createTasks(JobContext jobContext) {
    val tasks = ImmutableMap.<Task, TaskContext> builder();
    if (DownloadDataType.hasClinicalDataTypes(jobContext.getDataTypes())) {
      tasks.put(createClinical(jobContext));
    }

    tasks.putAll(createNonClinicalTasks(jobContext));

    return tasks.build();
  }

  private static Map<? extends Task, TaskContext> createNonClinicalTasks(JobContext jobContext) {
    val dataTypes = filterOutClinicalDataTypes(jobContext.getDataTypes());

    return dataTypes.stream()
        .collect(toImmutableMap(dt -> getTask(dt), dt -> createTaskContext(jobContext, singleton(dt))));
  }

  private static Task getTask(DownloadDataType dataType) {
    if (dataType == DownloadDataType.SSM_OPEN) {
      return new SsmTask(ImmutableSet.of(Marking.OPEN, Marking.MASKED));
    }

    if (dataType == DownloadDataType.SSM_CONTROLLED) {
      return new SsmTask(ImmutableSet.of(Marking.OPEN, Marking.CONTROLLED));
    }

    if (dataType.getFirstLevelFields().isEmpty() == false) {
      return new SecondaryTask();
    }

    return new GenericTask();
  }

  private static Entry<? extends Task, ? extends TaskContext> createClinical(JobContext jobContext) {
    val dataTypes = filterClinical(jobContext.getDataTypes());
    val taskContext = createTaskContext(jobContext, dataTypes);

    return Maps.immutableEntry(new ClinicalTask(), taskContext);
  }

  private static Set<DownloadDataType> filterClinical(Set<DownloadDataType> dataTypes) {
    return Sets.intersection(CLINICAL, dataTypes);
  }

  private static Set<DownloadDataType> filterOutClinicalDataTypes(Set<DownloadDataType> dataTypes) {
    val genericDataTypes = Sets.newHashSet(dataTypes);
    genericDataTypes.removeAll(CLINICAL);

    return genericDataTypes;
  }

  private static TaskContext createTaskContext(JobContext jobContext, Set<DownloadDataType> dataTypes) {
    return new TaskContext(
        jobContext.getJobId(),
        jobContext.getInputDir(),
        jobContext.getOutputDir(),
        jobContext.getDonorIds(),
        dataTypes,
        jobContext.getSparkContext());
  }

}
