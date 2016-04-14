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

import static com.google.common.collect.Maps.immutableEntry;
import static org.apache.spark.JobExecutionStatus.FAILED;
import static org.apache.spark.JobExecutionStatus.SUCCEEDED;
import static org.icgc.dcc.common.core.util.Separators.DASH;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.server.utils.JobProgresses.createJobProgress;
import static org.icgc.dcc.download.server.utils.Responses.verifyJobExistance;

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
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.model.JobProgress;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.model.TaskProgress;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.util.DownloadJobs;
import org.icgc.dcc.download.job.core.DownloadJob;
import org.icgc.dcc.download.job.core.JobContext;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.endpoint.NotFoundException;
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

    // FIXME: does not actually cancels SparkJob
    future.cancel(true);

    val job = jobRepository.findById(jobId);
    verifyJobExistance(job, jobId);
    val cancelledJob = Jobs.cancelJob(job);
    jobRepository.save(cancelledJob);
  }

  public Map<String, JobProgress> getJobsStatus(@NonNull Set<String> jobIds) {
    return jobIds.stream()
        .map(jobId -> immutableEntry(jobId, getJobStatus(jobId)))
        .filter(entry -> entry.getValue() != null)
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  public Map<String, JobInfo> getJobsInfo(@NonNull Set<String> jobIds) {
    return jobIds.stream()
        .map(jobId -> jobRepository.findById(jobId))
        .filter(job -> job != null)
        .collect(toImmutableMap(job -> job.getId(), job -> job.getJobInfo()));
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
    val activeJob = Jobs.setActiveDownload(job);
    jobRepository.save(activeJob);
  }

  private JobProgress getJobStatus(String jobId) {
    val job = jobRepository.findById(jobId);
    if (job == null) {
      return null;
    }

    val jobStatus = job.getStatus();
    val dataTypes = job.getDataTypes();

    // FIXME: Fix calculation for running job, which may be submitted, but not yet running because Spark is still
    // initializing task for it

    return jobStatus == JobStatus.RUNNING ?
        calculateJobProgress(job.getId(), dataTypes) :
        createJobProgress(jobStatus, dataTypes);
  }

  private JobProgress calculateJobProgress(String jobId, Set<DownloadDataType> dataTypes) {
    // TODO: Calculate progress in a separate thread and put it to the database.

    val taskProgress = dataTypes.stream()
        .collect(toImmutableMap(dt -> dt, dt -> calculateTaskProgress(jobId, dt)));

    return new JobProgress(JobStatus.RUNNING, taskProgress);
  }

  private TaskProgress calculateTaskProgress(String jobId, DownloadDataType dataType) {
    val jobName = DownloadJobs.getJobName(jobId, dataType);
    log.debug("Calculating job status for job '{}'", jobName);
    val statusTracker = sparkContext.statusTracker();
    int[] jobIds = statusTracker.getJobIdsForGroup(jobName);
    int totalJobs = jobIds.length;
    log.debug("[{}] Total jobs: {}", jobName, totalJobs);

    if (totalJobs == 0) {
      throw new NotFoundException("Failed to find job " + jobName);
    }

    val completedJobs = calculateCompletedJobs(statusTracker, jobIds);
    log.debug("[{}] Completed jobs: {}", jobName, completedJobs);

    val completedPercentage = completedJobs / totalJobs;
    log.debug("[{}] Completed jobs %: {}", jobName, completedPercentage);

    val response = new TaskProgress(completedJobs, totalJobs);
    log.debug("[{}] {}", jobName, response);

    return response;
  }

  private static int calculateCompletedJobs(JavaSparkStatusTracker statusTracker, int[] jobIds) {
    int completed = 0;

    for (val jobId : jobIds) {
      val jobInfo = statusTracker.getJobInfo(jobId);
      val jobStatus = jobInfo.status();
      if (jobStatus.equals(SUCCEEDED) || jobStatus.equals(FAILED)) {
        completed++;
      }
    }

    return completed;
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
