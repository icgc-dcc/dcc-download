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
package org.icgc.dcc.download.core.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@Data
@JsonInclude(Include.NON_NULL)
public class Job {

  private String id;
  private Set<String> donorIds;
  private Set<DownloadDataType> dataTypes;
  private JobStatus status;
  private Long submissionDate;
  private Long completionDate;

  private JobUiInfo jobInfo;

  private Long fileSizeBytes;
  private Integer ttlHours;

  private Map<DownloadDataType, TaskProgress> progress;

  public static JobBuilder builder() {
    return new JobBuilder();
  }

  // Can't use lombok @Builder because it removes the public no-args Job constructor required by the Jackson
  public static class JobBuilder {

    private final Job job = new Job();

    public JobBuilder id(String id) {
      job.id = id;

      return this;
    }

    public JobBuilder donorIds(Set<String> donorIds) {
      job.donorIds = donorIds;

      return this;
    }

    public JobBuilder dataTypes(Set<DownloadDataType> dataTypes) {
      job.dataTypes = dataTypes;

      return this;
    }

    public JobBuilder status(JobStatus status) {
      job.status = status;

      return this;
    }

    public JobBuilder submissionDate(Long submissionDate) {
      job.submissionDate = submissionDate;

      return this;
    }

    public JobBuilder completionDate(Long completionDate) {
      job.completionDate = completionDate;

      return this;
    }

    public JobBuilder jobInfo(JobUiInfo jobInfo) {
      job.jobInfo = jobInfo;

      return this;
    }

    public JobBuilder fileSizeBytes(Long fileSizeBytes) {
      job.fileSizeBytes = fileSizeBytes;

      return this;
    }

    public JobBuilder ttlHours(Integer ttlHours) {
      job.ttlHours = ttlHours;

      return this;
    }

    public JobBuilder progress(Map<DownloadDataType, TaskProgress> progress) {
      job.progress = progress;

      return this;
    }

    public Job build() {
      return job;
    }

  }

}
