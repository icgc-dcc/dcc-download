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
package org.icgc.dcc.download.server.utils;

import static com.google.common.collect.Sets.difference;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.download.core.model.DownloadDataType.CLINICAL;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;

import java.time.Duration;
import java.util.Set;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.server.model.Job;

import com.google.common.collect.ImmutableSet;

@NoArgsConstructor(access = PRIVATE)
public final class Jobs {

  public static final Duration ARCHIVE_TTL = Duration.ofHours(48);

  public static Job createJob(@NonNull String jobId, @NonNull SubmitJobRequest request) {
    return Job.builder()
        .id(jobId)
        .donorIds(request.getDonorIds())
        .dataTypes(refineClinicalDataTypes(request.getDataTypes()))
        .jobInfo(setTtl(request.getJobInfo()))
        .userEmailAddress(request.getUserEmailAddress())
        .status(JobStatus.RUNNING)
        .build();
  }

  private static JobInfo setTtl(JobInfo jobInfo) {
    jobInfo.setTtl((int) ARCHIVE_TTL.toHours());

    return jobInfo;
  }

  public static Job completeJob(@NonNull Job job, long archiveSize) {
    val completionDate = getDateAsMillis();
    job.setCompletionDate(completionDate);
    job.setStatus(JobStatus.SUCCEEDED);
    val jobInfo = job.getJobInfo();
    jobInfo.setCompletionTime(completionDate);
    jobInfo.setFileSize(archiveSize);

    return job;
  }

  public static Job failJob(@NonNull Job job) {
    job.setCompletionDate(getDateAsMillis());
    job.setStatus(JobStatus.FAILED);

    return job;
  }

  public static Job cancelJob(@NonNull Job job) {
    job.setStatus(JobStatus.KILLED);

    return job;
  }

  public static Job setActiveDownload(@NonNull Job job) {
    job.setStatus(JobStatus.ACTIVE_DOWNLOAD);

    return job;
  }

  public static Job unsetActiveDownload(@NonNull Job job) {
    job.setStatus(JobStatus.SUCCEEDED);

    return job;
  }

  private static long getDateAsMillis() {
    return System.currentTimeMillis();
  }

  private static Set<DownloadDataType> refineClinicalDataTypes(Set<DownloadDataType> dataTypes) {
    return dataTypes.contains(DownloadDataType.DONOR) == false ?
        dataTypes :
        ImmutableSet.<DownloadDataType> builder()
            .addAll(difference(dataTypes, CLINICAL))
            .add(DONOR)
            .build();
  }

}
