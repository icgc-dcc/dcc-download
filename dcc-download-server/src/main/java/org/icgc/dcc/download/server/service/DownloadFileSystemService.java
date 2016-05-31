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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.download.server.fs.AbstractDownloadFileSystem.DATA_DIR;
import static org.icgc.dcc.download.server.utils.DownloadFileSystems.isReleaseDir;
import static org.icgc.dcc.download.server.utils.DownloadFileSystems.toDfsPath;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.common.collect.ImmutableMap;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DownloadFileSystems;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

@Slf4j
public class DownloadFileSystemService {

  /**
   * Row - release; Column - donor; Cell - project.<br>
   * <b>NB:</b> Don't use directly. Call getDonorProjects()
   */
  private final Table<String, String, String> releaseDonorProjects = HashBasedTable.<String, String, String> create();
  private final Map<String, Table<String, DownloadDataType, DataTypeFile>> releaseFileTypes;

  public DownloadFileSystemService(String rootDir, FileSystem fileSystem) {
    this.releaseFileTypes = createReleaseFileTypes(rootDir, fileSystem);
  }

  public long getReleaseTime(@NonNull String release) {
    // TODO: implement
    return 0L;
  }

  private Map<String, String> getProjectDonors(String release) {
    val donorProjects = releaseDonorProjects.row(release);
    if (donorProjects == null || donorProjects.isEmpty()) {
      createDonorProjects(release);
      return releaseDonorProjects.row(release);
    }

    return donorProjects;
  }

  private void createDonorProjects(String release) {
    val fileTypes = releaseFileTypes.get(release);
  }

  private Map<String, Table<String, DownloadDataType, DataTypeFile>> createReleaseFileTypes(String rootDir,
      FileSystem fileSystem) {
    val releaseFileTypes = ImmutableMap.<String, Table<String, DownloadDataType, DataTypeFile>> builder();
    val releases = HadoopUtils.lsDir(fileSystem, new Path(rootDir));
    for (val release : releases) {
      val releaseName = toDfsPath(release).replace("/", EMPTY_STRING);
      releaseFileTypes.put(releaseName, createReleaseCache(release, fileSystem));
    }

    return releaseFileTypes.build();
  }

  static Table<String, DownloadDataType, DataTypeFile> createReleaseCache(Path releasePath, FileSystem fileSystem) {
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
