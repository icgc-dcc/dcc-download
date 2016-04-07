package org.icgc.dcc.download.client;

import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.icgc.dcc.download.client.io.ArchiveOutputStream;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.model.JobStatusResponse;

@RequiredArgsConstructor
public class DownloadClient {

  /**
   * Dependencies.
   */
  @NonNull
  private final ArchiveOutputStream outputStream;
  @NonNull
  private final HttpDownloadClient httpClient;

  public String submitJob(@NonNull Set<String> donorIds, @NonNull Set<DownloadDataType> dataTypes) {
    return httpClient.submitJob(donorIds, dataTypes);
  }

  public void cancelJob(@NonNull String jobId) {
    httpClient.cancelJob(jobId);
  }

  public JobStatusResponse getJobStatus(@NonNull String jobId) {
    return httpClient.getJobStatus(jobId);
  }

  public void setActiveDownload(@NonNull String jobId) {
    httpClient.setActiveDownload(jobId);
  }

  public void unsetActiveDownload(@NonNull String jobId) {
    httpClient.unsetActiveDownload(jobId);
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

    return outputStream.streamArchiveInGzTar(managedOut, downloadId, downloadDataTypes);
  }

}
