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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.job.core.DownloadJob;
import org.icgc.dcc.download.job.core.JobContext;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.model.Job;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.icgc.dcc.download.test.AbstractSparkTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class DownloadServiceTest extends AbstractSparkTest {

  @Mock
  JobProperties jobProperties;
  @Mock
  JobRepository jobRepository;
  @Mock
  Job job;

  CompletionService<String> completionService = createCompletionService();
  Map<String, Future<String>> submittedJobs = Maps.newHashMap();

  DownloadService downloadService;

  @Override
  public void setUp() {
    super.setUp();
    this.downloadService = new DownloadService(sparkContext, fileSystem, jobProperties, completionService,
        jobRepository, submittedJobs, new TestArchiveJob());
  }

  @Test
  @Ignore
  public void testCancelJob() throws Exception {
    val submitRequest1 = createSubmitJobRequest(singleton("1"), singleton(DownloadDataType.DONOR));
    val jobId = downloadService.submitJob(submitRequest1);
    when(jobRepository.findById(jobId)).thenReturn(job);
    log.warn("Job ID: {}", jobId);
    downloadService.cancelJob(jobId);
    fail("Finish!");
  }

  @Test
  @Ignore("For manual tests only")
  public void testWaitJob() throws Exception {
    val submitRequest1 = createSubmitJobRequest(singleton("1"), singleton(DownloadDataType.DONOR));
    val submitRequest2 = createSubmitJobRequest(singleton("2"), singleton(DownloadDataType.DONOR));

    downloadService.submitJob(submitRequest1);
    downloadService.submitJob(submitRequest2);

    Thread.sleep(5_000L);
    reportStats();

    log.info("Press any button to exit.");
    System.in.read();
  }

  @Test
  public void testGetJobsStatus() throws Exception {
    val submitRequest = createSubmitJobRequest(singleton("1"), singleton(DownloadDataType.DONOR));
    val jobId = downloadService.submitJob(submitRequest);

    when(jobRepository.findById(jobId)).thenReturn(job);
    when(job.getStatus()).thenReturn(JobStatus.SUCCEEDED);
    when(job.getDataTypes()).thenReturn(singleton(DownloadDataType.DONOR));

    val statuses = downloadService.getJobsStatus(Collections.singleton(jobId));
    log.info("{}", statuses);

    assertThat(statuses).hasSize(1);
    val status = statuses.get(jobId);
    assertThat(status.getStatus()).isEqualTo(JobStatus.SUCCEEDED);
    val taskProgresses = status.getTaskProgress();
    assertThat(taskProgresses).hasSize(1);
    val donorProgress = taskProgresses.get(DownloadDataType.DONOR);
    assertThat(donorProgress.getNumerator()).isEqualTo(1L);
    assertThat(donorProgress.getDenominator()).isEqualTo(1L);
  }

  private SubmitJobRequest createSubmitJobRequest(Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    val submitRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(dataTypes)
        .build();
    return submitRequest;
  }

  private void reportStats() {
    val statusTracker = sparkContext.statusTracker();

    int[] groupJobs = statusTracker.getJobIdsForGroup("test_gr1");
    log.info("Group job ids: {}", groupJobs);

    for (val activeJob : groupJobs) {
      val jobInfo = statusTracker.getJobInfo(activeJob);
      log.info("JobInfo: {}", jobInfo);
      val status = jobInfo.status();
      log.info("Job status: {}", status);
    }

    int[] activeJobs = statusTracker.getActiveJobIds();
    log.info("Active job ids: {}", activeJobs);

    for (val activeJob : activeJobs) {
      val jobInfo = statusTracker.getJobInfo(activeJob);
      log.info("JobInfo: {}", jobInfo);
      val status = jobInfo.status();
      log.info("Job status: {}", status);
    }

  }

  private static CompletionService<String> createCompletionService() {
    val executor = Executors.newFixedThreadPool(1);

    return new ExecutorCompletionService<String>(executor);
  }

  public static class TestArchiveJob implements DownloadJob {

    @Override
    @SneakyThrows
    public void execute(JobContext jobContext) {
      val sparkContext = jobContext.getSparkContext();
      val jobId = jobContext.getJobId();
      sparkContext.setJobGroup(jobId, jobId);

      val rdd = sparkContext.parallelize(ImmutableList.of(1, 2, 3, 4, 5))
          .mapToPair(n -> new Tuple2<Integer, Integer>(n, 1));
      rdd.cache();
      rdd.count();

      int counter = 0;

      while (counter++ < 2) {
        Thread.sleep(3_000L);
        log.info("Running...");
      }

      log.info("Done");
    }

  }

}
