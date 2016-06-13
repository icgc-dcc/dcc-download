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

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.collect.Maps.immutableEntry;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.Separators.DASH;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.core.model.JobStatus.EXPIRED;
import static org.icgc.dcc.download.core.model.JobStatus.SUCCEEDED;
import static org.icgc.dcc.download.server.utils.DataTypeFiles.getDownloadDataType;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.HEADERS_DIR;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.JobResponse;
import org.icgc.dcc.download.server.io.ArchiveStreamer;
import org.icgc.dcc.download.server.io.GzipStreamer;
import org.icgc.dcc.download.server.io.TarStreamer;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.model.Job;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

@Slf4j
@Service
@RequiredArgsConstructor
public class ArchiveDownloadService {

  @NonNull
  private final Path rootPath;
  @NonNull
  private final FileSystemService fileSystemService;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final JobRepository jobRepository;

  public String submitDownloadRequest(SubmitJobRequest request) {
    val downloadFiles = getDataTypeFiles(request);
    val jobId = generateId();
    val job = Job.builder()
        .fileSizeBytes(resolveFileSize(downloadFiles))
        .donorIds(request.getDonorIds())
        .dataTypes(request.getDataTypes())
        .dataFiles(downloadFiles)
        .id(jobId)
        .jobInfo(request.getJobInfo())
        .status(SUCCEEDED)
        .submissionDate(request.getSubmissionTime())
        .build();
    jobRepository.save(job);

    return jobId;
  }

  public Map<DownloadDataType, Long> getFilesSize(@NonNull Collection<String> donorIds) {
    val downloadDataTypes = ImmutableSet.copyOf(DownloadDataType.values());
    val dataTypes = getDataTypeFiles(donorIds, downloadDataTypes);

    return dataTypes.stream()
        .map(dataType -> immutableEntry(getDownloadDataType(dataType), dataType.getTotalSize()))
        .collect(() -> Maps.<DownloadDataType, Long> newHashMap(),
            ArchiveDownloadService::accumulate,
            ArchiveDownloadService::combine);

  }

  public Optional<JobResponse> getArchiveInfo(@NonNull String jobId) {
    val job = jobRepository.findById(jobId);
    if (job == null || job.getStatus() == EXPIRED) {
      return Optional.empty();
    }

    return Optional.of(new JobResponse(job.getJobInfo(), job.getFileSizeBytes()));
  }

  public Optional<ArchiveStreamer> getArchiveStreamer(@NonNull String jobId, @NonNull OutputStream output) {
    val job = jobRepository.findById(jobId);
    if (job == null || job.getStatus() == EXPIRED) {
      return Optional.empty();
    }

    val release = fileSystemService.getCurrentRelease();
    val downloadFiles = job.getDataFiles();
    val dataTypes = job.getDataTypes();

    return Optional.of(getArchiveStreamer(release, downloadFiles, dataTypes, output));
  }

  public ArchiveStreamer getArchiveStreamer(
      @NonNull String release,
      @NonNull List<DataTypeFile> downloadFiles,
      @NonNull Collection<DownloadDataType> dataTypes,
      @NonNull OutputStream output) {
    // Resolve size for each dataType
    val fileSizes = resolveFileSizes(downloadFiles);

    // Resolve headers for the dataTypes
    val headers = resolveHeaders(release, dataTypes);

    // Stream back as gzip if single dataType, as tar if multiple datatypes
    // during streaming insert headers and create tar entities when required.

    // Convert OutputStream to InputStream http://blog.ostermiller.org/convert-java-outputstream-inputstream
    if (headers.size() == 1) {
      return getGzipStreamer(downloadFiles, fileSizes, headers, output);
    } else {
      return getTarStreamer(downloadFiles, fileSizes, headers, output);
    }
  }

  private ArchiveStreamer getTarStreamer(List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, String> headers, OutputStream output) {
    val tarOut = createTarOutputStream(output);
    val gzipStreamer = getGzipStreamer(downloadFiles, fileSizes, headers, tarOut);
    return new TarStreamer(tarOut, gzipStreamer);
  }

  private GzipStreamer getGzipStreamer(List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, String> headers, OutputStream output) {
    return new GzipStreamer(fileSystem, downloadFiles, fileSizes, headers, output);
  }

  private Map<DownloadDataType, String> resolveHeaders(String release, Collection<DownloadDataType> dataTypes) {
    return dataTypes.stream()
        .collect(toImmutableMap(dataType -> dataType, dataType -> getHeaderPath(release, dataType)));

  }

  private String getHeaderPath(String release, DownloadDataType dataType) {
    val headerPath = PATH.join(rootPath, release, HEADERS_DIR, dataType.getId() + ".tsv");

    return headerPath;
  }

  private List<DataTypeFile> getDataTypeFiles(SubmitJobRequest request) {
    val donors = request.getDonorIds();
    val dataTypes = request.getDataTypes();

    return getDataTypeFiles(donors, dataTypes);
  }

  private List<DataTypeFile> getDataTypeFiles(Collection<String> donors, Collection<DownloadDataType> dataTypes) {
    val release = fileSystemService.getCurrentRelease();

    return fileSystemService.getDataTypeFiles(release, donors, dataTypes);
  }

  private static Map<DownloadDataType, Long> resolveFileSizes(List<DataTypeFile> downloadFiles) {
    val fileSizes = Maps.<DownloadDataType, Long> newHashMap();
    for (val file : downloadFiles) {
      val type = resolveType(file);
      val size = firstNonNull(fileSizes.get(type), 0L);
      fileSizes.put(type, size + file.getTotalSize());
    }

    return ImmutableMap.copyOf(fileSizes);
  }

  private static DownloadDataType resolveType(DataTypeFile file) {
    val name = new Path(file.getPath()).getName();

    return DownloadDataType.valueOf(name.toUpperCase());
  }

  // TODO: explicitly specify apache-common dependency
  private static TarArchiveOutputStream createTarOutputStream(OutputStream out) {
    log.debug("Creating tar output stream...");
    val tarOut = new TarArchiveOutputStream(out);
    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOut;
  }

  private static String generateId() {
    return UUID.randomUUID()
        .toString()
        .replace(DASH, EMPTY_STRING);
  }

  private static Long resolveFileSize(List<DataTypeFile> downloadFiles) {
    // TODO: Calculate tar overhead
    return resolveFileSizes(downloadFiles).values().stream()
        .mapToLong(value -> value)
        .sum();
  }

  private static void accumulate(Map<DownloadDataType, Long> accumulator, Map.Entry<DownloadDataType, Long> entry) {
    val type = entry.getKey();
    val value = entry.getValue();
    val currentValue = accumulator.get(type);
    accumulator.put(type, add(currentValue, value));
  }

  private static void combine(Map<DownloadDataType, Long> left, Map<DownloadDataType, Long> right) {
    right.entrySet().stream()
        .forEach(e -> accumulate(left, e));
  }

  private static Long add(Long currentValue, Long value) {
    return currentValue == null ? value : currentValue + value;
  }

}
