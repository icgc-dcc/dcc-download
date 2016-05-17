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

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Future;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.SparkStageInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.Job;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.model.TaskProgress;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.job.core.DownloadJob;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DownloadServiceTest {

  private static final String JOB_ID = "123";
  private static final String SPARK_JOB_NAME = JOB_ID + "-DONOR";

  @Mock
  JavaSparkContext sparkContext;
  @Mock
  JavaSparkStatusTracker statusTracker;
  @Mock
  SparkJobInfo sparkJobInfo1;
  @Mock
  SparkJobInfo sparkJobInfo2;
  @Mock
  SparkJobInfo sparkJobInfo3;
  @Mock
  SparkStageInfo sparkStageInfo;
  @Mock
  SparkStageInfo uncompleteSparkStageInfo;
  @Mock
  FileSystem fs;

  @Mock
  JobProperties jobProperties;
  @Mock
  JobRepository jobRepository;

  @Mock
  DownloadJob downloadJob;
  @Mock
  Future<String> jobFuture;

  @Mock
  CompletionService<String> completionService;
  @Mock
  Map<String, Future<String>> submittedJobs;

  DownloadService downloadService;

  @Before
  public void setUp() {
    this.downloadService = new DownloadService(sparkContext, fs, jobProperties, completionService,
        jobRepository, submittedJobs, downloadJob);
  }

  @Test
  public void testSubmitJob() throws Exception {
    val submitRequest1 = createSubmitJobRequest(singleton("1"), singleton(DownloadDataType.DONOR));
    val jobId = downloadService.submitJob(submitRequest1);

    assertThat(jobId).isNotNull();
    verify(submittedJobs, times(1)).put(eq(jobId), any());
    verify(jobRepository, times(1)).save(any(Job.class));
    verify(completionService, times(1)).submit(Matchers.<Callable<String>> any());
  }

  @Test
  public void testCancelJob() throws Exception {
    when(submittedJobs.remove(JOB_ID)).thenReturn(jobFuture);
    when(jobRepository.findById(JOB_ID)).thenReturn(createTestJob(JobStatus.RUNNING));

    downloadService.cancelJob(JOB_ID);

    verify(jobFuture).cancel(true);
    verify(sparkContext).cancelJobGroup(SPARK_JOB_NAME);
    val expecrtedJob = createTestJob(JobStatus.RUNNING);
    expecrtedJob.setStatus(JobStatus.KILLED);
    verify(jobRepository, times(1)).save(expecrtedJob);
  }

  @Test
  public void testSetActiveDownload() throws Exception {
    when(jobRepository.findById(JOB_ID)).thenReturn(createTestJob(JobStatus.RUNNING));

    downloadService.setActiveDownload(JOB_ID);

    val expecrtedJob = createTestJob(JobStatus.RUNNING);
    expecrtedJob.setStatus(JobStatus.TRANSFERRING);
    verify(jobRepository, times(1)).save(expecrtedJob);
  }

  @Test
  public void testUnsetActiveDownload() throws Exception {
    when(jobRepository.findById(JOB_ID)).thenReturn(createTestJob(JobStatus.RUNNING));

    downloadService.unsetActiveDownload(JOB_ID);

    val expecrtedJob = createTestJob(JobStatus.RUNNING);
    expecrtedJob.setStatus(JobStatus.SUCCEEDED);
    verify(jobRepository, times(1)).save(expecrtedJob);
  }

  @Test
  public void testGetJob_notFound() throws Exception {
    val job = downloadService.getJob("", false);
    assertThat(job).isNull();
  }

  @Test
  public void testGetJob_noProgress() throws Exception {
    val job = createTestJob(null);
    when(jobRepository.findById(JOB_ID)).thenReturn(job);
    val actualJob = downloadService.getJob(JOB_ID, false);
    assertThat(actualJob).isEqualTo(job);
  }

  @Test
  public void testGetJob_progress_completed() throws Exception {
    val dbJob = createTestJob(JobStatus.SUCCEEDED);
    when(jobRepository.findById(JOB_ID)).thenReturn(dbJob);

    val actualJob = downloadService.getJob(JOB_ID, true);
    val expectedJob = createTestJob(JobStatus.SUCCEEDED);
    expectedJob.setProgress(ImmutableMap.of(DONOR, new TaskProgress(1, 1)));

    log.info("Actual job: {}", actualJob);
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  @Test
  public void testGetJob_progress_failed() throws Exception {
    val dbJob = createTestJob(JobStatus.FAILED);
    when(jobRepository.findById(JOB_ID)).thenReturn(dbJob);

    val actualJob = downloadService.getJob(JOB_ID, true);
    val expectedJob = createTestJob(JobStatus.FAILED);
    expectedJob.setProgress(ImmutableMap.of(DONOR, new TaskProgress(0, 0)));

    log.info("Actual job: {}", actualJob);
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  @Test
  public void testGetJob_progress_running() throws Exception {
    val dbJob = createTestJob(JobStatus.RUNNING);
    when(jobRepository.findById(JOB_ID)).thenReturn(dbJob);
    mockGetJobStatus();

    val actualJob = downloadService.getJob(JOB_ID, true);
    val expectedJob = createTestJob(JobStatus.RUNNING);
    expectedJob.setProgress(ImmutableMap.of(DONOR, new TaskProgress(2, 3)));

    log.info("Actual job: {}", actualJob);
    assertThat(actualJob).isEqualTo(expectedJob);
  }

  private void mockGetJobStatus() {
    when(sparkContext.statusTracker()).thenReturn(statusTracker);

    // 3 Jobs are running
    when(statusTracker.getJobIdsForGroup(SPARK_JOB_NAME)).thenReturn(new int[] { 1, 2, 3 });

    // mock getStages()
    when(statusTracker.getJobInfo(1)).thenReturn(sparkJobInfo1);
    when(sparkJobInfo1.stageIds()).thenReturn(new int[] { 1 });
    when(statusTracker.getJobInfo(2)).thenReturn(sparkJobInfo2);
    when(sparkJobInfo2.stageIds()).thenReturn(new int[] { 2 });
    when(statusTracker.getJobInfo(3)).thenReturn(sparkJobInfo3);
    when(sparkJobInfo3.stageIds()).thenReturn(new int[] { 3 });

    // mock getTotalTasks()
    when(statusTracker.getStageInfo(1)).thenReturn(sparkStageInfo);
    when(sparkStageInfo.numTasks()).thenReturn(1);
    when(statusTracker.getStageInfo(2)).thenReturn(sparkStageInfo);
    when(sparkStageInfo.numTasks()).thenReturn(1);
    when(statusTracker.getStageInfo(3)).thenReturn(uncompleteSparkStageInfo);
    when(uncompleteSparkStageInfo.numTasks()).thenReturn(1);

    // mock getCompletedTasks()
    when(sparkStageInfo.numCompletedTasks()).thenReturn(1);
    when(uncompleteSparkStageInfo.numCompletedTasks()).thenReturn(0);
  }

  private SubmitJobRequest createSubmitJobRequest(Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    val submitRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(dataTypes)
        .jobInfo(JobUiInfo.builder().build())
        .build();
    return submitRequest;
  }

  private static Job createTestJob(JobStatus status) {
    return Job.builder()
        .id(JOB_ID)
        .status(status)
        .dataTypes(singleton(DONOR))
        .build();
  }

}
