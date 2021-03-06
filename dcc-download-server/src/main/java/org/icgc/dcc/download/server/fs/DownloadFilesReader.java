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
package org.icgc.dcc.download.server.fs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.Splitters.PATH;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsDir;
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_PATTERN;
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_PREFIX;
import static org.icgc.dcc.download.server.utils.DfsPaths.toDfsPath;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;
import static org.icgc.dcc.download.server.utils.Releases.isLegacyRelease;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.endpoint.NotFoundException;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DfsPaths;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

@Slf4j
@RequiredArgsConstructor
public class DownloadFilesReader {

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final PathResolver pathResolver;

  @Getter(lazy = true)
  private final Map<String, Table<String, DownloadDataType, DataTypeFile>> releaseDonorFileTypes = // NOPMD
      createReleaseFileTypes();

  public Map<String, Multimap<String, String>> getReleaseProjectDonors() {
    return getReleaseProjectDonors(getReleaseDonorFileTypes());
  }

  public Map<String, Long> getReleaseTimes() {
    val releaseTimes = ImmutableMap.<String, Long> builder();
    val releaseDirs = getReleasePaths(Optional.of(compile(".*" + RELEASE_DIR_PREFIX + ".*")));
    log.debug("Resolving creation time for release dirs: {}", releaseDirs);
    for (val releaseDir : releaseDirs) {
      val status = getFileStatus(fileSystem, releaseDir);
      releaseTimes.put(releaseDir.getName(), status.getModificationTime());
    }

    return releaseTimes.build();
  }

  public long getReleaseTime(@NonNull String releaseName) {
    checkArgument(isValidReleaseName(releaseName));
    val releasePath = getReleasePath(releaseName);
    val status = getFileStatus(fileSystem, releasePath);

    return status.getModificationTime();
  }

  public Table<String, DownloadDataType, DataTypeFile> createReleaseCache(@NonNull String releaseName) {
    checkArgument(isValidReleaseName(releaseName));
    val releasePath = getReleasePath(releaseName);

    return createReleaseCache(releasePath);
  }

  public boolean isValidReleaseName(@NonNull String releaseName) {
    return RELEASE_DIR_PATTERN.matcher(releaseName).matches();
  }

  public static Map<String, Multimap<String, String>> getReleaseProjectDonors(
      @NonNull Map<String, Table<String, DownloadDataType, DataTypeFile>> releaseDonorFileTypes) {
    val releaseProjectDonors = ImmutableMap.<String, Multimap<String, String>> builder();
    for (val entry : releaseDonorFileTypes.entrySet()) {
      val release = entry.getKey();
      val fileTypes = entry.getValue();
      releaseProjectDonors.put(release, createProjectDonors(fileTypes));
    }

    return releaseProjectDonors.build();
  }

  static Multimap<String, String> createProjectDonors(Table<String, DownloadDataType, DataTypeFile> donorFileTypes) {
    val projectDonors = ArrayListMultimap.<String, String> create();
    val donors = donorFileTypes.column(DONOR);
    for (val entry : donors.entrySet()) {
      val donorId = entry.getKey();
      val project = resolveProject(entry.getValue());
      projectDonors.put(project, donorId);
    }

    return projectDonors;
  }

  Table<String, DownloadDataType, DataTypeFile> createReleaseCache(Path releasePath) {
    // Expected number of donors.
    val releaseTable = HashBasedTable.<String, DownloadDataType, DataTypeFile> create(18000, 2);

    val releaseDirs = HadoopUtils.lsDir(fileSystem, releasePath);
    checkState(DfsPaths.isReleaseDir(releaseDirs), "'%s' is not the release dir.");

    val release = releasePath.getName();
    log.info("Recursively reading files for release '{}'", release);
    val watch = Stopwatch.createStarted();
    val dataDirPath = new Path(releasePath, DATA_DIR);
    processPath(fileSystem, releaseTable, dataDirPath);
    log.info("Populated cache table for release '{}' in {} seconds.", release, watch.elapsed(SECONDS));

    return releaseTable;
  }

  private Path getReleasePath(String releaseName) {
    val releasePaths = getReleasePaths(Optional.of(Pattern.compile(releaseName)));
    if (releasePaths.size() != 1) {
      log.warn("Failed to resolve release path for release {}. Release paths: {}", releaseName, releasePaths);
      throw new NotFoundException(format("Release '%s' does not exist", releaseName));
    }

    return releasePaths.get(0);
  }

  private Map<String, Table<String, DownloadDataType, DataTypeFile>> createReleaseFileTypes() {
    log.info("Creating donor - download data type - data type file tables...");
    val releaseFileTypes = ImmutableMap.<String, Table<String, DownloadDataType, DataTypeFile>> builder();
    val releases = getReleasePaths(Optional.empty());
    log.info("Release paths to process: {}", releases);

    for (val release : releases) {
      val timer = Stopwatch.createStarted();
      checkState(RELEASE_DIR_PATTERN.matcher(release.getName()).matches());
      val releaseName = toDfsPath(release).replace("/", EMPTY_STRING);
      log.info("Creating donor - download data type - data type file table for release '{}'", releaseName);
      releaseFileTypes.put(releaseName, createReleaseCache(release));
      log.info("Created data type files cache for release '{}' in {}", releaseName, timer.elapsed(SECONDS));
    }

    return releaseFileTypes.build();
  }

  private List<Path> getReleasePaths(Optional<Pattern> pattern) {
    val allPaths = pattern.isPresent() ?
        lsDir(fileSystem, pathResolver.getRootPath(), pattern.get()) :
        lsDir(fileSystem, pathResolver.getRootPath());

    return allPaths.stream()
        .filter(path -> !isLegacyRelease(fileSystem, path))
        .collect(toImmutableList());
  }

  @SneakyThrows
  private void processPath(
      FileSystem fileSystem,
      HashBasedTable<String, DownloadDataType, DataTypeFile> releaseTable,
      Path dirPath) {
    for (val fileStatus : fileSystem.listStatus(dirPath)) {
      if (fileStatus.isDirectory()) {
        processPath(fileSystem, releaseTable, fileStatus.getPath());
      } else {
        addFile(fileSystem, releaseTable, fileStatus);
      }
    }
  }

  private void addFile(
      FileSystem fileSystem,
      HashBasedTable<String, DownloadDataType, DataTypeFile> releaseTable,
      FileStatus fileStatus) {
    // No need to convert to URI as the path schema is removed when the path is used.
    val filePath = fileStatus.getPath();
    if (filePath.getName().equals("_SUCCESS")) {
      return;
    }

    log.debug("Processing file '{}'", filePath);
    val dfsPath = toDfsPath(filePath);
    log.debug("DFS path: {}", dfsPath);

    val fileParts = getFileParts(dfsPath);
    val donorId = fileParts.get(0);
    val dataType = getDataType(fileParts.get(1));

    val dataTypeFile = releaseTable.get(donorId, dataType);
    val updatedDataTypeFile = updateDataTypeFile(dataTypeFile, fileStatus);
    log.debug("Adding {}", updatedDataTypeFile);
    releaseTable.put(donorId, dataType, updatedDataTypeFile);
  }

  private DataTypeFile updateDataTypeFile(DataTypeFile dataTypeFile, FileStatus fileStatus) {
    val fileSize = fileStatus.getLen();
    val filePath = fileStatus.getPath();
    if (dataTypeFile == null) {
      return createDataTypeFile(filePath, fileSize);
    }

    return updateDataTypeFile(dataTypeFile, filePath.getName(), fileSize);
  }

  private DataTypeFile updateDataTypeFile(DataTypeFile dataTypeFile, String partFile, long fileSize) {
    val path = dataTypeFile.getPath();
    val partFiles = Sets.newTreeSet(dataTypeFile.getPartFileIndices());
    partFiles.add(pathResolver.getPartFileIndex(partFile));
    val totalSize = dataTypeFile.getTotalSize() + fileSize;

    return new DataTypeFile(path, ImmutableList.copyOf(partFiles), totalSize);
  }

  private DataTypeFile createDataTypeFile(Path file, long fileSize) {
    return new DataTypeFile(
        pathResolver.getDataFilePath(file.toString()),
        Collections.singletonList(pathResolver.getPartFileIndex(file.getName())),
        fileSize);
  }

  private static String resolveProject(DataTypeFile dataTypeFile) {
    val path = dataTypeFile.getPath();
    log.debug("Resolving project from path: '{}'", path);

    val pathParts = PATH.splitToList(path);
    // Path should look like TST1-CA/DO1/donor
    checkState(pathParts.size() == 3, "Failed to resolve project from path '%s'. Parts: '%s'", path, pathParts);

    val projectId = pathParts.get(0);
    log.debug("Resolved project '{}' from path '{}'", projectId, path);

    return projectId;
  }

  private static List<String> getFileParts(String dfsPath) {
    val parts = Splitters.PATH.splitToList(dfsPath);
    checkState(parts.size() == 7, "Parts: %s", parts);

    return parts.subList(4, parts.size());
  }

  private static DownloadDataType getDataType(String dataType) {
    return DownloadDataType.valueOf(dataType.toUpperCase(ENGLISH));
  }

}
