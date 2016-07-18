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
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Splitters.UNDERSCORE;
import static org.icgc.dcc.download.server.model.Export.DATA;
import static org.icgc.dcc.download.server.model.Export.RELEASE;
import static org.icgc.dcc.download.server.model.Export.REPOSITORY;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;

import java.io.OutputStream;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.io.DataExportStreamer;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.io.RealFileStreamer;
import org.icgc.dcc.download.server.io.ReleaseExportStreamer;
import org.icgc.dcc.download.server.model.Export;
import org.icgc.dcc.download.server.model.ExportFile;
import org.icgc.dcc.download.server.model.MetadataResponse;
import org.springframework.beans.factory.annotation.Autowired;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsService {

  private static final Pattern RELEASE_NAME_PATTERN = Pattern.compile("^icgc(\\d+)-.*");
  private static final String ES_EXPORT_DIR = "es_export";

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final String exportsPath;
  @NonNull
  private final String dataDir;

  public MetadataResponse getMetadata(@NonNull String baseUrl) {
    val repositoryFile = createRepositoryMeta(baseUrl);
    val dataFile = createDataMeta(baseUrl);
    val releaseFile = createReleaseMeta(baseUrl);

    return new MetadataResponse(repositoryFile, dataFile, releaseFile);
  }

  public FileStreamer getExportStreamer(@NonNull Export export, @NonNull OutputStream output) {
    switch (export) {
    case REPOSITORY:
      return new RealFileStreamer(new Path(exportsPath, export.getId()), fileSystem, output);
    case DATA:
      return new DataExportStreamer(new Path(dataDir), fileSystem, output);
    case RELEASE:
      val releaseNumber = resolveReleaseNumber();
      val exportId = RELEASE.getId(releaseNumber);
      val releaseExportsPath = getReleaseExportsPath();

      return new ReleaseExportStreamer(exportId, releaseExportsPath, fileSystem, output);
    default:
      throw new IllegalArgumentException(format("Failed to resolve streamer for ID '%s'", export.getId()));
    }
  }

  private ExportFile createDataMeta(String baseUrl) {
    val creationDate = getFileModificationDate(dataDir);

    return createFileMetadata(DATA, baseUrl, creationDate);
  }

  private ExportFile createRepositoryMeta(@NonNull String baseUrl) {
    val creationDate = getFileModificationDate(exportsPath + "/" + REPOSITORY.getId());

    return createFileMetadata(REPOSITORY, baseUrl, creationDate);
  }

  private ExportFile createReleaseMeta(String baseUrl) {
    val releaseNumber = resolveReleaseNumber();
    val exportId = RELEASE.getId(releaseNumber);
    val filePath = getExportFilePath(baseUrl, exportId);
    val creationDate = resolveReleaseCreationDate();

    return new ExportFile(
        filePath,
        exportId,
        RELEASE,
        creationDate);
  }

  private int resolveReleaseNumber() {
    val releaseFile = getReleaseFirstFile();
    val fileName = releaseFile.getName();
    val fileNameParts = UNDERSCORE.splitToList(fileName);
    val errorMessage = format("Failed to resolve relase number from file '%s'", fileName);
    checkState(fileNameParts.size() == 2, errorMessage);
    val release = fileNameParts.get(0);
    val matcher = RELEASE_NAME_PATTERN.matcher(release);
    checkState(matcher.matches(), errorMessage);
    val releaseNumberString = matcher.group(1);

    return Integer.parseInt(releaseNumberString);
  }

  private long resolveReleaseCreationDate() {
    val releaseFile = getReleaseFirstFile();
    val fileStatus = getFileStatus(fileSystem, releaseFile);

    return fileStatus.getModificationTime();
  }

  private Path getReleaseFirstFile() {
    val releaseExportsPath = getReleaseExportsPath();
    val releaseFiles = HadoopUtils.lsFile(fileSystem, releaseExportsPath);
    checkState(!releaseFiles.isEmpty(), "Release files directory is empty.");

    val releaseFile = releaseFiles.get(0);
    return releaseFile;
  }

  private Path getReleaseExportsPath() {
    return new Path(exportsPath, ES_EXPORT_DIR);
  }

  private long getFileModificationDate(String filePath) {
    val path = new Path(filePath);
    val status = getFileStatus(fileSystem, path);

    return status.getModificationTime();
  }

  private static ExportFile createFileMetadata(Export export, String baseUrl, long creationDate) {
    return new ExportFile(
        getExportFilePath(baseUrl, export.getId()),
        export.getId(),
        export,
        creationDate);
  }

  private static String getExportFilePath(String baseUrl, String id) {
    return baseUrl + "/exports/" + id;
  }

}
