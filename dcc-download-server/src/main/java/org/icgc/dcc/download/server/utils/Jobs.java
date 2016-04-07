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

import static lombok.AccessLevel.PRIVATE;

import java.util.Date;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.download.core.model.JobStatus;
import org.icgc.dcc.download.server.model.Job;

@NoArgsConstructor(access = PRIVATE)
public final class Jobs {

  public static Job createJob(@NonNull String jobId) {
    return Job.builder()
        .id(jobId)
        .submissionDate(getDateAsMillis())
        .status(JobStatus.RUNNING)
        .build();
  }

  public static Job completeJob(@NonNull Job job) {
    job.setCompletionDate(getDateAsMillis());
    job.setStatus(JobStatus.COMPLETED);

    return job;
  }

  public static Job cancelJob(@NonNull Job job) {
    job.setStatus(JobStatus.CANCELLED);

    return job;
  }

  public static Job setActiveDownload(@NonNull Job job) {
    job.setStatus(JobStatus.ACTIVE_DOWNLOAD);

    return job;
  }

  public static Job unsetActiveDownload(@NonNull Job job) {
    job.setStatus(JobStatus.COMPLETED);

    return job;
  }

  private static long getDateAsMillis() {
    return new Date().getTime();
  }

}
