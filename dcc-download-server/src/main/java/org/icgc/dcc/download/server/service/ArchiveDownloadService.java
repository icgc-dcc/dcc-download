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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.immutableEntry;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.Separators.DASH;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.server.utils.DataTypeFiles.getDownloadDataType;
import static org.icgc.dcc.download.server.utils.DfsPaths.getFileName;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.HEADERS_DIR;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.PROJECTS_FILES;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.SUMMARY_FILES;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.request.SubmitJobRequest;
import org.icgc.dcc.download.core.response.JobResponse;
import org.icgc.dcc.download.server.fs.PathResolver;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.io.GzipStreamer;
import org.icgc.dcc.download.server.io.RealFileStreamer;
import org.icgc.dcc.download.server.io.TarStreamer;
import org.icgc.dcc.download.server.model.DataFiles;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.model.Job;
import org.icgc.dcc.download.server.repository.DataFilesRepository;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.icgc.dcc.download.server.utils.DfsPaths;
import org.icgc.dcc.download.server.utils.Responses;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ArchiveDownloadService {

  @NonNull
  private final Path rootPath;
  @NonNull
  private final FileSystemService fileSystemService;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final JobRepository jobRepository;
  @NonNull
  private final DataFilesRepository dataFilesRepository;
  @NonNull
  private final PathResolver pathResolver;

  public String submitDownloadRequest(SubmitJobRequest request) {
    val downloadFiles = getDataTypeFiles(request);
    val jobId = generateId();
    val job = Job.builder()
        .fileSizeBytes(resolveFileSize(downloadFiles))
        .donorIds(request.getDonorIds())
        .dataTypes(request.getDataTypes())
        .id(jobId)
        .jobInfo(request.getJobInfo())
        .submissionDate(request.getSubmissionTime())
        .build();

    jobRepository.save(job);
    dataFilesRepository.save(new DataFiles(jobId, fileSystemService.getCurrentRelease(), downloadFiles));

    return jobId;
  }

  public Map<DownloadDataType, Long> getFilesSize(@NonNull Collection<String> donorIds) {
    val downloadDataTypes = ImmutableSet.copyOf(DownloadDataType.values());
    log.debug("Resolving data files...");
    val dataTypes = getUnsortedDataTypeFiles(donorIds, downloadDataTypes);
    log.debug("Resolved file sizes.");

    return dataTypes.stream()
        .map(dataType -> immutableEntry(getDownloadDataType(dataType), dataType.getTotalSize()))
        .collect(() -> Maps.<DownloadDataType, Long> newHashMap(),
            ArchiveDownloadService::accumulate,
            ArchiveDownloadService::combine);
  }

  public Optional<JobResponse> getArchiveInfo(@NonNull String jobId) {
    val job = jobRepository.findById(jobId);
    if (job == null) {
      return Optional.empty();
    }

    return Optional.of(new JobResponse(
        jobId,
        job.getJobInfo(),
        job.getDataTypes(),
        job.getFileSizeBytes(),
        job.getSubmissionDate())
        );
  }

  public Optional<FileStreamer> getArchiveStreamer(@NonNull String jobId, @NonNull OutputStream output) {
    val job = jobRepository.findById(jobId);
    if (job == null) {
      return Optional.empty();
    }

    val release = fileSystemService.getCurrentRelease();
    val downloadFiles = dataFilesRepository.findById(jobId).getDataFiles();
    val dataTypes = job.getDataTypes();

    return Optional.of(getArchiveStreamer(release, downloadFiles, dataTypes, output, emptyMap()));
  }

  public Optional<FileStreamer> getArchiveStreamer(
      @NonNull String jobId,
      @NonNull OutputStream output,
      @NonNull DownloadDataType dataType) {
    val job = jobRepository.findById(jobId);
    if (job == null) {
      return Optional.empty();
    }

    val dataTypes = job.getDataTypes();
    // TODO: check what is returned to the client
    checkState(dataTypes.contains(dataType), "Download ID '%s' does not have data type '%s' for download.", jobId,
        dataType);

    val release = fileSystemService.getCurrentRelease();
    val downloadFiles = filterDataFiles(dataFilesRepository.findById(jobId).getDataFiles(), dataType);

    return Optional.of(getArchiveStreamer(release, downloadFiles, dataTypes, output, emptyMap()));
  }

  public Optional<FileStreamer> getStaticArchiveStreamer(@NonNull String path, @NonNull OutputStream output) {
    if (isLegacyFile(path) || DfsPaths.isRealEntity(path)) {
      log.info("'{}' represents a real file...", path);

      return getRealFileStreamer(path, output);
    }

    DfsPaths.validatePath(path);
    log.info("'{}' is a synthetic file", path);
    String release = DfsPaths.getRelease(path);
    release = release.equals("current") ? fileSystemService.getCurrentRelease() : release;
    val projects = getProject(DfsPaths.getProject(path), release);
    log.info("Getting data files for projects: {}", projects);
    val downloadDataType = DfsPaths.getDownloadDataType(path);
    val downloadFiles = fileSystemService.getDataTypeFiles(release, projects, downloadDataType);
    val fileNames = resolveFileNames(downloadDataType, projects);

    if (downloadFiles.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(getArchiveStreamer(release, downloadFiles, singleton(downloadDataType), output, fileNames));
  }

  public boolean isUserDownload(@NonNull String id, @NonNull String user) {
    val job = jobRepository.findById(id);
    if (job == null) {
      return false;
    }

    val allowedUser = job.getJobInfo().getUser();

    return allowedUser.equals(user);
  }

  /**
   * Checks if the file represented by the {@code path} is located in the legacy release directory.
   */
  private boolean isLegacyFile(String path) {
    log.debug("Checking if '{}' is a legacy file...", path);
    val release = DfsPaths.getLegacyRelease(path);

    return fileSystemService.isLegacyRelease(release);
  }

  private Set<String> getProject(Optional<String> project, String release) {
    if (project.isPresent()) {
      return Collections.singleton(project.get());
    }

    Optional<List<String>> projectsOpt = fileSystemService.getReleaseProjects(release);
    if (!projectsOpt.isPresent()) {
      Responses.throwPathNotFoundException(format("Failed to resolve projects for release '%s'", release));
    }

    return ImmutableSet.copyOf(projectsOpt.get());
  }

  private Optional<FileStreamer> getRealFileStreamer(String path, OutputStream output) {
    val filePath = getRealFilePath(path, isLegacyFile(path));
    log.info("Resolved download path '{}' to actual path '{}'", path, filePath);
    if (!HadoopUtils.exists(fileSystem, filePath)) {
      log.warn("Download path '{}' doesn't exist", filePath);

      return Optional.empty();
    }

    log.info("Creating file streamer for '{}'", filePath);

    return Optional.of(new RealFileStreamer(filePath, fileSystem, output));
  }

  private Path getRealFilePath(String path, boolean legacy) {
    return legacy ?
        new Path(rootPath, path.replaceFirst("^/", EMPTY_STRING)) :
        getRealFilePath(path);
  }

  private Path getRealFilePath(String path) {
    log.debug("Resolving real path for file '{}'", path);
    val filePath = new Path(rootPath, path
        .replaceFirst("current", fileSystemService.getCurrentRelease())
        .replaceFirst("Summary", SUMMARY_FILES)
        .replaceFirst("Projects", PROJECTS_FILES)
        .replaceFirst("^/", EMPTY_STRING));
    log.debug("Resolved '{}' to '{}'", path, filePath);

    return filePath;
  }

  private FileStreamer getArchiveStreamer(
      @NonNull String release,
      @NonNull List<DataTypeFile> downloadFiles,
      @NonNull Collection<DownloadDataType> dataTypes,
      @NonNull OutputStream output,
      @NonNull Map<DownloadDataType, String> fileNames) {
    // Resolve size for each dataType
    val fileSizes = resolveFileSizes(downloadFiles);

    // Resolve headers for the dataTypes
    val headers = resolveHeaders(release, dataTypes);

    // Stream back as gzip if single dataType, as tar if multiple datatypes
    // during streaming insert headers and create tar entities when required.

    if (isSingleEntry(headers)) {
      log.info("Creating gzip streamer for data types {}", dataTypes);
      return getGzipStreamer(downloadFiles, fileSizes, headers, output, release, fileNames);
    } else {
      log.info("Creating tar streamer for data types {}", dataTypes);
      return getTarStreamer(downloadFiles, fileSizes, headers, output, release);
    }
  }

  private boolean isSingleEntry(Map<DownloadDataType, String> headers) {
    return headers.size() == 1;
  }

  private FileStreamer getTarStreamer(List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, String> headers, OutputStream output, String release) {
    val tarOut = createTarOutputStream(output);
    val gzipStreamer = getGzipStreamer(downloadFiles, fileSizes, headers, tarOut, release, emptyMap());
    return new TarStreamer(tarOut, gzipStreamer);
  }

  private GzipStreamer getGzipStreamer(
      List<DataTypeFile> downloadFiles,
      Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, String> headers,
      OutputStream output,
      String release,
      Map<DownloadDataType, String> fileNames) {
    return new GzipStreamer(fileSystem, downloadFiles, fileSizes, headers, output, pathResolver, release, fileNames);
  }

  private Map<DownloadDataType, String> resolveHeaders(String release, Collection<DownloadDataType> dataTypes) {
    return dataTypes.stream()
        .collect(toImmutableMap(dataType -> dataType, dataType -> getHeaderPath(release, dataType)));

  }

  private String getHeaderPath(String release, DownloadDataType dataType) {
    val headerPath = PATH.join(rootPath, release, HEADERS_DIR, dataType.getId() + ".tsv.gz");

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

  private List<DataTypeFile> getUnsortedDataTypeFiles(Collection<String> donors, Collection<DownloadDataType> dataTypes) {
    val release = fileSystemService.getCurrentRelease();

    return fileSystemService.getUnsortedDataTypeFiles(release, donors, dataTypes);
  }

  private static List<DataTypeFile> filterDataFiles(List<DataTypeFile> dataFiles, DownloadDataType dataType) {
    return dataFiles.stream()
        .filter(dataFile -> {
          DownloadDataType downloadDataType = getDownloadDataType(dataFile);

          return (dataType == DONOR && downloadDataType.isClinicalSubtype()) || downloadDataType == dataType;
        })
        .collect(toImmutableList());

  }

  private static Map<DownloadDataType, Long> resolveFileSizes(List<DataTypeFile> downloadFiles) {
    val fileSizes = Maps.<DownloadDataType, Long> newHashMap();
    for (val file : downloadFiles) {
      val type = resolveType(file);
      val size = firstNonNull(fileSizes.get(type), 0L);
      fileSizes.put(type, size + file.getTotalSize());
    }

    return fileSizes.entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .collect(toImmutableMap(entry -> entry.getKey(), entry -> entry.getValue()));
  }

  private static DownloadDataType resolveType(DataTypeFile file) {
    val name = new Path(file.getPath()).getName();

    return DownloadDataType.valueOf(name.toUpperCase());
  }

  private static TarArchiveOutputStream createTarOutputStream(OutputStream outputStream) {
    log.debug("Creating tar output stream...");
    val tarOutputStream = new TarArchiveOutputStream(outputStream);
    tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOutputStream;
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

  private static Map<DownloadDataType, String> resolveFileNames(DownloadDataType downloadDataType, Set<String> projects) {
    val fileSuffix = projects.size() == 1 ? format(".%s", projects.iterator().next()) : ".all_projects";
    val fileName = getFileName(downloadDataType, Optional.of(fileSuffix)) + ".tsv.gz";

    return singletonMap(downloadDataType, fileName);
  }

}
