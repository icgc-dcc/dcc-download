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
package org.icgc.dcc.download.server.task;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.core.model.JobStatus.EXPIRED;
import static org.icgc.dcc.download.core.model.JobStatus.TRANSFERRING;
import static org.icgc.dcc.download.server.utils.Jobs.ARCHIVE_TTL;

import java.time.Instant;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.Job;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("production")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RemoveExpiredJobs {

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final JobRepository jobRepository;
  @NonNull
  private final String outputDir;

  // Daily at midnight
  @Scheduled(cron = "0 0 0 * * *")
  public void execute() {
    log.info("Removing expired jobs artifacts...");
    val expiredJob = getExpiredJobs();
    log.info("Expired jobs count: {}", expiredJob.size());

    expiredJob.stream()
        .forEach(job -> deleteJobFiles(job.getId()));
  }

  List<Job> getExpiredJobs() {
    return jobRepository.findByCompletionDateLessThanAndStatusNot(getExpirationDate(), EXPIRED).stream()
        .filter(job -> job.getStatus() != TRANSFERRING)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private void deleteJobFiles(String jobId) {
    val path = getPath(jobId);
    if (HadoopUtils.exists(fileSystem, path)) {
      fileSystem.delete(path, true);
      log.info("Removed job '{}' directory.", jobId);
    } else {
      log.warn("Failed to clean job '{}'. The job directory does not exist.", jobId);
    }
  }

  private Path getPath(String jobId) {
    return new Path(outputDir, jobId);
  }

  private static long getExpirationDate() {
    val expirationDate = Instant.now().minus(ARCHIVE_TTL);

    return expirationDate.toEpochMilli();
  }

}
