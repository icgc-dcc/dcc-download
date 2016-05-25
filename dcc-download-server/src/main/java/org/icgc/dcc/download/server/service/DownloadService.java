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
package org.icgc.dcc.download.server.service;

import static java.util.Arrays.stream;
import static org.icgc.dcc.common.core.util.Separators.DASH;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.server.utils.Responses.verifyJobExistance;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.model.TaskProgress;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.util.DownloadJobs;
import org.icgc.dcc.download.job.core.DownloadJob;
import org.icgc.dcc.download.job.core.JobContext;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.icgc.dcc.download.server.utils.Jobs;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class DownloadService {

  /**
   * Dependencies.
   */
  @NonNull
  private final JavaSparkContext sparkContext;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final JobProperties jobProperties;
  @NonNull
  private final CompletionService<String> completionService;
  @NonNull
  private final JobRepository jobRepository;
  @NonNull
  private final Map<String, Future<String>> submittedJobs;
  @NonNull
  private final DownloadJob archiveJob;

  public String submitJob(@NonNull SubmitJobRequest request) {
    val jobId = getJobId();
    val donorIds = request.getDonorIds();
    val dataTypes = request.getDataTypes();
    val jobContext = createJobContext(jobId, donorIds, dataTypes);

    val future = submitJob(jobId, jobContext);
    submittedJobs.put(jobId, future);

    val job = Jobs.createJob(jobId, request);
    jobRepository.save(job);
    log.info("Submitted job '{}'.", jobId);

    return jobId;
  }

  public void cancelJob(@NonNull String jobId) {
    val future = submittedJobs.remove(jobId);
    if (future == null) {
      log.debug("Failed to cancel job {}. It has been alreading completed.", jobId);
      return;
    }

    val job = jobRepository.findById(jobId);
    verifyJobExistance(job, jobId);
    future.cancel(true);
    cancelSparkJobs(jobId, job.getDataTypes());
    val cancelledJob = Jobs.cancelJob(job);
    jobRepository.save(cancelledJob);
  }

  public Job getJob(String jobId, boolean includeProgress) {
    val job = jobRepository.findById(jobId);
    log.debug("Found job in the DB: {}", job);
    if (job == null) {
      return null;
    }

    if (!includeProgress) {
      return job;
    }

    val jobStatus = job.getStatus();
    val dataTypes = job.getDataTypes();
    val progress = jobStatus == JobStatus.RUNNING ?
        calculateJobProgress(job.getId(), dataTypes) :
        Jobs.createJobProgress(jobStatus, dataTypes);
    job.setProgress(progress);

    return job;
  }

  public void setActiveDownload(@NonNull String jobId) {
    val job = jobRepository.findById(jobId);
    verifyJobExistance(job, jobId);
    val activeJob = Jobs.setActiveDownload(job);
    jobRepository.save(activeJob);
  }

  public void unsetActiveDownload(@NonNull String jobId) {
    val job = jobRepository.findById(jobId);
    verifyJobExistance(job, jobId);
    val activeJob = Jobs.unsetActiveDownload(job);
    jobRepository.save(activeJob);
  }

  private Map<DownloadDataType, TaskProgress> calculateJobProgress(String jobId, Set<DownloadDataType> dataTypes) {
    return dataTypes.stream()
        .collect(toImmutableMap(dt -> dt, dt -> calculateTaskProgress(jobId, dt)));
  }

  private void cancelSparkJobs(String jobId, Set<DownloadDataType> dataTypes) {
    dataTypes.stream()
        .forEach(type -> cancelSparkJob(jobId, type));
  }

  private void cancelSparkJob(String jobId, DownloadDataType dataType) {
    val jobName = DownloadJobs.getJobName(jobId, dataType);
    log.info("Cancelling job group '{}'", jobName);
    sparkContext.cancelJobGroup(jobName);
  }

  private TaskProgress calculateTaskProgress(String jobId, DownloadDataType dataType) {
    val jobName = DownloadJobs.getJobName(jobId, dataType);
    log.debug("Calculating job status for job '{}'", jobName);
    val statusTracker = sparkContext.statusTracker();
    int[] jobIds = statusTracker.getJobIdsForGroup(jobName);
    int totalJobs = jobIds.length;
    log.debug("[{}] Total jobs: {}", jobName, totalJobs);

    if (totalJobs <= 2) {
      // It takes 2 Spark jobs just to read parquet files to identify their metadata. These jobs will be just ignored
      return new TaskProgress(0, 1);
    }

    val stages = getStages(statusTracker, jobIds);
    val totalTasks = getTotalTasks(statusTracker, stages);
    val completedTasks = getCompletedTasks(statusTracker, stages);
    val response = new TaskProgress(completedTasks, totalTasks);
    log.debug("[{}] {}", jobName, response);

    return response;
  }

  private JobContext createJobContext(String jobId, Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    return new JobContext(
        jobId,
        donorIds,
        dataTypes,
        sparkContext,
        fileSystem,
        jobProperties.getInputDir(),
        jobProperties.getOutputDir());
  }

  private static List<Integer> getStages(JavaSparkStatusTracker statusTracker, int[] jobIds) {
    return stream(jobIds)
        .flatMap(jobId -> stream(statusTracker.getJobInfo(jobId).stageIds()))
        .boxed()
        .collect(toImmutableList());
  }

  private static int getTotalTasks(JavaSparkStatusTracker statusTracker, List<Integer> stages) {
    return stages.stream()
        .mapToInt(id -> statusTracker.getStageInfo(id).numTasks())
        .sum();
  }

  private static int getCompletedTasks(JavaSparkStatusTracker statusTracker, List<Integer> stages) {
    return stages.stream()
        .mapToInt(id -> statusTracker.getStageInfo(id).numCompletedTasks())
        .sum();
  }

  private static String getJobId() {
    return UUID.randomUUID()
        .toString()
        .replace(DASH, EMPTY_STRING);
  }

  private Future<String> submitJob(final String jobId, JobContext jobContext) {
    return completionService.submit(new Callable<String>() {

      @Override
      public String call() throws Exception {
        archiveJob.execute(jobContext);

        return jobId;
      }
    });
  }

}
