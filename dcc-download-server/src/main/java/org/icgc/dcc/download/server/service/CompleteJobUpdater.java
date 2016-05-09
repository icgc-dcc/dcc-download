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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.server.utils.Jobs.completeJob;
import static org.icgc.dcc.download.server.utils.Jobs.failJob;

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.mail.Mailer;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class CompleteJobUpdater implements Runnable {

  /**
   * Dependencies.
   */
  @Autowired
  private CompletionService<String> completionService;
  @Autowired
  private JobRepository repository;
  @Resource(name = "submittedJobs")
  private Map<String, Future<String>> submittedJobs;
  @Autowired
  private ArchiveSizeService archiveSizeService;
  @Autowired
  private Mailer mailer;

  @PostConstruct
  public void startDaemon() {
    val thread = new Thread(this);
    thread.setDaemon(true);
    thread.start();
  }

  @Override
  @SneakyThrows
  public void run() {
    Future<String> result = null;
    while (true) {
      result = completionService.take();
      try {
        processSuccessfulJob(result);
      } catch (CancellationException e) {
        log.debug("Job was cancelled: ", e);
      } catch (ExecutionException e) {
        processFailedJob(result, e);
      } catch (InterruptedException e) {
        log.info("Job was cancelled: ", e);
      }
    }
  }

  private void processSuccessfulJob(Future<String> result) throws InterruptedException, ExecutionException {
    val jobId = result.get();
    log.info("Job '{}' is completed.", jobId);

    val job = findJob(jobId);
    val archiveSize = archiveSizeService.getArchiveSize(jobId);

    val completedJob = completeJob(job, archiveSize);
    repository.save(completedJob);
    log.debug("Updated job {}", completedJob);

    submittedJobs.remove(jobId);
    mailer.sendSuccessful(jobId, job.getUserEmailAddress());
  }

  private void processFailedJob(Future<String> result, ExecutionException e) {
    log.error("Encountered an error while executing job.\n", e);
    val jobId = getJobIdByFuture(result);
    if (jobId != null) {
      val job = findJob(jobId);

      val failedJob = failJob(job);
      repository.save(failedJob);
      log.debug("Updated job {}", failedJob);

      submittedJobs.remove(jobId);
      log.info("Removed failed jobID '{}'", jobId);
      mailer.sendFailed(jobId, job.getUserEmailAddress());
    }
  }

  private org.icgc.dcc.download.server.model.Job findJob(final java.lang.String jobId) {
    log.debug("Looking for job to update...");
    val job = repository.findById(jobId);
    log.debug("Updating {} ...", job);
    return job;
  }

  private String getJobIdByFuture(Future<String> jobResult) {
    if (jobResult == null) {
      log.error("Failed to resolve jobId from null future.");

      return null;
    }

    val jobsIds = submittedJobs.entrySet().stream()
        .filter(e -> jobResult.equals(e.getValue()))
        .map(e -> e.getKey())
        .collect(toImmutableList());
    checkState(jobsIds.size() < 2, "Resolved more that one failed jobs for the same Future.");

    if (jobsIds.isEmpty()) {
      log.error("Failed to find jobId in the submitted jobs list.");

      return null;
    }

    return jobsIds.get(0);
  }

}
