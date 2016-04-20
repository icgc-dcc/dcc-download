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
package org.icgc.dcc.download.client;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.download.core.model.DownloadDataType.CLINICAL;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.download.client.io.ArchiveOutputStream;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobInfo;
import org.icgc.dcc.download.core.model.JobProgress;
import org.icgc.dcc.download.core.request.SubmitJobRequest;

import com.google.common.collect.ImmutableSet;

@RequiredArgsConstructor
public class DownloadClient {

  /**
   * Dependencies.
   */
  @NonNull
  private final ArchiveOutputStream outputStream;
  @NonNull
  private final HttpDownloadClient httpClient;

  // TODO: Externalize
  public String defaultEmail = "";

  public boolean isServiceAvailable() {
    // TODO: Implement!
    return true;
  }

  public String submitJob(
      @NonNull Set<String> donorIds,
      @NonNull Set<DownloadDataType> dataTypes,
      @NonNull JobInfo jobInfo,
      @NonNull String userEmailAddress) {
    val submitDataTypes = resolveDataTypes(donorIds, dataTypes);
    val submitJobRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(submitDataTypes)
        .jobInfo(jobInfo)
        .userEmailAddress(userEmailAddress)
        .build();

    return httpClient.submitJob(submitJobRequest);
  }

  public void cancelJob(@NonNull String jobId) {
    httpClient.cancelJob(jobId);
  }

  public Map<String, JobProgress> getJobsProgress(@NonNull Set<String> jobIds) {
    return httpClient.getJobsProgress(jobIds);
  }

  public Map<String, JobInfo> getJobsInfo(@NonNull Set<String> jobIds) {
    return httpClient.getJobsInfo(jobIds);
  }

  public void setActiveDownload(@NonNull String jobId) {
    httpClient.setActiveDownload(jobId);
  }

  public void unsetActiveDownload(@NonNull String jobId) {
    httpClient.unsetActiveDownload(jobId);
  }

  public Map<DownloadDataType, Long> getSizes(@NonNull Set<String> donorIds) {
    val requestBody = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .build();

    return httpClient.getSizes(requestBody);
  }

  @SneakyThrows
  public boolean streamArchiveInGz(@NonNull OutputStream out, @NonNull String downloadId,
      @NonNull DownloadDataType dataType) {
    @Cleanup
    val managedOut = out;

    return outputStream.streamArchiveInGz(managedOut, downloadId, dataType);
  }

  @SneakyThrows
  public boolean streamArchiveInTarGz(@NonNull OutputStream out, @NonNull String downloadId,
      @NonNull List<DownloadDataType> downloadDataTypes) {
    @Cleanup
    val managedOut = out;

    return outputStream.streamArchiveInTarGz(managedOut, downloadId, downloadDataTypes);
  }

  private Set<DownloadDataType> resolveDataTypes(Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    val submitDataTypes = resolveSubmitDataTypes(dataTypes);
    val sizes = getSizes(donorIds);

    return submitDataTypes.stream()
        .filter(dt -> sizes.get(dt) != null && sizes.get(dt) > 0L)
        .collect(toImmutableSet());
  }

  private static Set<DownloadDataType> resolveSubmitDataTypes(Set<DownloadDataType> dataTypes) {
    return dataTypes.contains(DONOR) ?
        ImmutableSet.<DownloadDataType> builder()
            .addAll(dataTypes)
            .addAll(CLINICAL)
            .build() :
        dataTypes;
  }

}
