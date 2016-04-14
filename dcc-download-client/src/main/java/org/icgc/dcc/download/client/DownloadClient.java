package org.icgc.dcc.download.client;

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
      @NonNull JobInfo jobInfo) {
    val submitJobRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(dataTypes)
        .jobInfo(jobInfo)
        .userEmailAddress(defaultEmail)
        .build();

    return httpClient.submitJob(submitJobRequest);
  }

  public String submitJob(
      @NonNull Set<String> donorIds,
      @NonNull Set<DownloadDataType> dataTypes,
      @NonNull JobInfo jobInfo,
      @NonNull String userEmailAddress) {
    val submitJobRequest = SubmitJobRequest.builder()
        .donorIds(donorIds)
        .dataTypes(dataTypes)
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

}
