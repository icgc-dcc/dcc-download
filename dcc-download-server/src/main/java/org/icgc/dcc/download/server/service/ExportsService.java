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
import static org.icgc.dcc.download.server.model.ExportEntity.DATA;
import static org.icgc.dcc.download.server.model.ExportEntity.REPOSITORY;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;

import java.io.OutputStream;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.download.server.io.DataExportStreamer;
import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.io.RealFileStreamer;
import org.icgc.dcc.download.server.model.ExportEntity;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsService {

  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final String repositoryDir;
  @NonNull
  private final String dataDir;

  public ArrayNode getMetadata(@NonNull String baseUrl) {
    val response = Jackson.DEFAULT.createArrayNode();
    response.add(createRepositoryMeta(baseUrl));
    response.add(createDataMeta(baseUrl));

    return response;
  }

  public FileStreamer getExportStreamer(@NonNull ExportEntity exportEntity, @NonNull OutputStream output) {
    switch (exportEntity) {
    case REPOSITORY:
      return new RealFileStreamer(new Path(repositoryDir, exportEntity.getId()), fileSystem, output);
    case DATA:
      return new DataExportStreamer(new Path(dataDir), fileSystem, output);
    default:
      throw new IllegalArgumentException(format("Failed to resolve streamer for ID '%s'", exportEntity.getId()));
    }
  }

  private ObjectNode createDataMeta(String baseUrl) {
    val creationDate = getFileModificationDate(dataDir);

    return createFileMetadata(DATA, baseUrl, creationDate);
  }

  private ObjectNode createRepositoryMeta(@NonNull String baseUrl) {
    val creationDate = getFileModificationDate(repositoryDir + "/" + REPOSITORY.getId());

    return createFileMetadata(REPOSITORY, baseUrl, creationDate);
  }

  private long getFileModificationDate(String filePath) {
    val path = new Path(filePath);
    val status = getFileStatus(fileSystem, path);

    return status.getModificationTime();
  }

  private static ObjectNode createFileMetadata(ExportEntity exportEntity, String baseUrl, long creationDate) {
    // date: file date
    // url: https://download.icgc.org/exports/release_21.tar.gz
    // id: [release_21.tar.gz | ]
    // type: [release | data | repository]

    val metadata = Jackson.DEFAULT.createObjectNode();
    metadata.put("id", exportEntity.getId());
    metadata.put("type", exportEntity.getType());
    metadata.put("url", baseUrl + "/exports/" + exportEntity.getId());
    metadata.put("date", creationDate);

    return metadata;
  }

}
