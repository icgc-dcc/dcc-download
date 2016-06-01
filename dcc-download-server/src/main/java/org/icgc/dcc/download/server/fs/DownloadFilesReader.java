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

import static com.google.common.base.Preconditions.checkState;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.Splitters.PATH;
import static org.icgc.dcc.download.server.fs.AbstractDownloadFileSystem.RELEASE_DIR_PREFIX;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;
import static org.icgc.dcc.download.server.utils.DownloadFileSystems.isReleaseDir;
import static org.icgc.dcc.download.server.utils.DownloadFileSystems.toDfsPath;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DownloadFileSystems;

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
  private final String rootDir;
  @NonNull
  private final FileSystem fileSystem;

  @Getter(lazy = true)
  private final Map<String, Table<String, DownloadDataType, DataTypeFile>> releaseDonorFileTypes =
      createReleaseFileTypes();

  public Map<String, Multimap<String, String>> getReleaseProjectDonors() {
    val releaseProjectDonors = ImmutableMap.<String, Multimap<String, String>> builder();
    for (val entry : getReleaseDonorFileTypes().entrySet()) {
      val release = entry.getKey();
      val fileTypes = entry.getValue();
      releaseProjectDonors.put(release, createProjectDonors(fileTypes));
    }

    return releaseProjectDonors.build();
  }

  public Map<String, Long> getReleaseTimes() {
    val releaseTimes = ImmutableMap.<String, Long> builder();
    val releaseDirs = HadoopUtils.lsDir(fileSystem, new Path(rootDir), compile(".*" + RELEASE_DIR_PREFIX + ".*"));
    log.debug("Resolving creation time for release dirs: {}", releaseDirs);
    for (val releaseDir : releaseDirs) {
      val status = getFileStatus(fileSystem, releaseDir);
      releaseTimes.put(releaseDir.getName(), status.getModificationTime());
    }

    return releaseTimes.build();
  }

  Multimap<String, String> createProjectDonors(Table<String, DownloadDataType, DataTypeFile> donorFileTypes) {
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
    log.debug("Creating cache table for '{}'", releasePath);
    val releaseTable = HashBasedTable.<String, DownloadDataType, DataTypeFile> create();
    val releaseDirs = HadoopUtils.lsDir(fileSystem, releasePath);
    checkState(isReleaseDir(releaseDirs), "'%s' is not the release dir.");
    val allFiles = HadoopUtils.lsRecursive(fileSystem, new Path(releasePath, DATA_DIR));
    for (val file : allFiles) {
      addFile(fileSystem, releaseTable, file);
    }

    return releaseTable;
  }

  private Map<String, Table<String, DownloadDataType, DataTypeFile>> createReleaseFileTypes() {
    val releaseFileTypes = ImmutableMap.<String, Table<String, DownloadDataType, DataTypeFile>> builder();
    val releases = HadoopUtils.lsDir(fileSystem, new Path(rootDir));
    for (val release : releases) {
      val releaseName = toDfsPath(release).replace("/", EMPTY_STRING);
      releaseFileTypes.put(releaseName, createReleaseCache(release));
    }

    return releaseFileTypes.build();
  }

  private static void addFile(
      FileSystem fileSystem,
      HashBasedTable<String, DownloadDataType, DataTypeFile> releaseTable,
      String file) {

    log.debug("Processing file '{}'", file);
    val dfsPath = DownloadFileSystems.toDfsPath(file);
    log.debug("DFS path: {}", dfsPath);

    val fileParts = getFileParts(dfsPath);
    val donorId = fileParts.get(0);
    val dataType = getDataType(fileParts.get(1));
    val partFile = fileParts.get(2);
    log.debug("Resolved:  donor - {}, data type - {}, part file - {}", donorId, dataType, partFile);

    val dataTypeFile = releaseTable.get(donorId, dataType);
    val updatedDataTypeFile = updateDataTypeFile(fileSystem, dataTypeFile, file, partFile);
    log.debug("Adding {}", updatedDataTypeFile);
    releaseTable.put(donorId, dataType, updatedDataTypeFile);
  }

  private static String resolveProject(DataTypeFile dataTypeFile) {
    val path = dataTypeFile.getPath();
    log.debug("Resolving project from path: '{}'", path);

    val dfsPath = toDfsPath(path);
    log.debug("DFS path: '{}'", dfsPath);

    val pathParts = PATH.splitToList(dfsPath);
    checkState(pathParts.size() == 5, "Failed to resolve project from path: '%s'", dfsPath);

    val projectId = pathParts.get(2);
    log.debug("Resolved project '{}' from path '{}'", projectId, dfsPath);

    return projectId;
  }

  private static DataTypeFile updateDataTypeFile(FileSystem fileSystem, DataTypeFile dataTypeFile, String file,
      String partFile) {
    val fileSize = getFileSize(fileSystem, file);
    if (dataTypeFile == null) {
      return createDataTypeFile(file, partFile, fileSize);
    }

    return updateDataTypeFile(dataTypeFile, partFile, fileSize);
  }

  private static DataTypeFile updateDataTypeFile(DataTypeFile dataTypeFile, String partFile, long fileSize) {
    val path = dataTypeFile.getPath();
    val partFiles = Sets.newTreeSet(dataTypeFile.getPartFiles());
    partFiles.add(partFile);
    val totalSize = dataTypeFile.getTotalSize() + fileSize;

    return new DataTypeFile(path, ImmutableList.copyOf(partFiles), totalSize);
  }

  private static DataTypeFile createDataTypeFile(String file, String partFile, long fileSize) {
    return new DataTypeFile(getCommonPath(file), Collections.singletonList(partFile), fileSize);
  }

  private static String getCommonPath(String file) {
    return file.replaceFirst("/part-\\d{5}.gz", EMPTY_STRING);
  }

  private static long getFileSize(FileSystem fileSystem, String file) {
    val statusOpt = HadoopUtils.getFileStatus(fileSystem, new Path(file));
    checkState(statusOpt.isPresent(), "File doesn't exist. '%s'", file);
    val status = statusOpt.get();

    return status.getLen();
  }

  private static List<String> getFileParts(String dfsPath) {
    val parts = Splitters.PATH.splitToList(dfsPath);
    checkState(parts.size() == 7, "Parts: %s", parts);

    return parts.subList(4, parts.size());
  }

  private static DownloadDataType getDataType(String dataType) {
    return DownloadDataType.valueOf(dataType.toUpperCase());
  }

}
