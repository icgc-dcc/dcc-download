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
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.json.Jackson;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class ExportsService {

  public static final String REPOSITORY_FILE_ID = "repository.tar.gz";
  public static final String DATA_FILE_ID = "data.tar";

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

  private ObjectNode createDataMeta(String baseUrl) {
    val creationDate = getFileModificationDate(dataDir);

    return createFileMetadata(DATA_FILE_ID, baseUrl, creationDate);
  }

  private ObjectNode createRepositoryMeta(@NonNull String baseUrl) {
    val creationDate = getFileModificationDate(repositoryDir + "/" + REPOSITORY_FILE_ID);

    return createFileMetadata(REPOSITORY_FILE_ID, baseUrl, creationDate);
  }

  private long getFileModificationDate(String filePath) {
    val path = new Path(filePath);
    val status = getFileStatus(fileSystem, path);

    return status.getModificationTime();
  }

  private static ObjectNode createFileMetadata(String id, String baseUrl, long creationDate) {
    // date: file date
    // url: https://download.icgc.org/exports/release_21.tar.gz
    // id: [release_21.tar.gz | ]
    // type: [release | data | repository]

    val metadata = Jackson.DEFAULT.createObjectNode();
    metadata.put("id", id);
    metadata.put("type", getTypeFromId(id));
    metadata.put("url", baseUrl + "/exports/" + id);
    metadata.put("date", creationDate);

    return metadata;
  }

  private static String getTypeFromId(String id) {
    if (id.contains("repository")) {
      return "repository";
    }

    if (id.contains("release")) {
      return "release";
    }

    if (id.contains("data")) {
      return "data";
    }

    throw new IllegalArgumentException(format("Failed to resolve type from entity id '%s'", id));
  }

}
