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

import static java.lang.String.format;
import static org.icgc.dcc.download.server.model.Export.DATA;
import static org.icgc.dcc.download.server.model.Export.REPOSITORY;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;

import java.io.OutputStream;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.server.io.DataExportStreamer;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.io.RealFileStreamer;
import org.icgc.dcc.download.server.model.Export;
import org.icgc.dcc.download.server.model.ExportFile;
import org.icgc.dcc.download.server.model.MetadataResponse;
import org.springframework.beans.factory.annotation.Autowired;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsService {

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final String repositoryDir;
  @NonNull
  private final String dataDir;

  public MetadataResponse getMetadata(@NonNull String baseUrl) {
    val repositoryFile = createRepositoryMeta(baseUrl);
    val dataFile = createDataMeta(baseUrl);

    return new MetadataResponse(repositoryFile, dataFile);
  }

  public FileStreamer getExportStreamer(@NonNull Export export, @NonNull OutputStream output) {
    switch (export) {
    case REPOSITORY:
      return new RealFileStreamer(new Path(repositoryDir, export.getId()), fileSystem, output);
    case DATA:
      return new DataExportStreamer(new Path(dataDir), fileSystem, output);
    default:
      throw new IllegalArgumentException(format("Failed to resolve streamer for ID '%s'", export.getId()));
    }
  }

  private ExportFile createDataMeta(String baseUrl) {
    val creationDate = getFileModificationDate(dataDir);

    return createFileMetadata(DATA, baseUrl, creationDate);
  }

  private ExportFile createRepositoryMeta(@NonNull String baseUrl) {
    val creationDate = getFileModificationDate(repositoryDir + "/" + REPOSITORY.getId());

    return createFileMetadata(REPOSITORY, baseUrl, creationDate);
  }

  private long getFileModificationDate(String filePath) {
    val path = new Path(filePath);
    val status = getFileStatus(fileSystem, path);

    return status.getModificationTime();
  }

  private static ExportFile createFileMetadata(Export export, String baseUrl, long creationDate) {
    return new ExportFile(
        baseUrl + "/exports/" + export.getId(),
        export.getId(),
        export,
        creationDate);
  }

}
