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
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.json.Jackson.asObjectNode;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;
import static org.icgc.dcc.download.server.model.ExportEntity.DATA;
import static org.icgc.dcc.download.server.model.ExportEntity.REPOSITORY;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ExportsServiceTest extends AbstractTest {

  private static final String BASE_URL = "http://localhost";

  ExportsService service;

  FileSystem fileSystem = getDefaultLocalFileSystem();

  @Override
  @Before
  public void setUp() {
    super.setUp();
    service = new ExportsService(fileSystem, workingDir.getAbsolutePath(), workingDir.getAbsolutePath());
  }

  @Test
  public void testGetMetadata() throws Exception {
    val repoFile = new File(workingDir, REPOSITORY.getId());
    repoFile.createNewFile();

    val meta = service.getMetadata(BASE_URL);

    log.info("{}", meta);
    assertThat(meta).hasSize(2);
    val creationTime = getFileStatus(fileSystem, new Path(workingDir.getAbsolutePath())).getModificationTime();

    val repoMeta = asObjectNode(meta.get(0));
    assertThat(repoMeta.get("id").asText()).isEqualTo(REPOSITORY.getId());
    assertThat(repoMeta.get("type").asText()).isEqualTo(REPOSITORY.getType());
    assertThat(repoMeta.get("url").asText()).isEqualTo(getIdUrl(REPOSITORY.getId()));
    assertThat(repoMeta.get("date").asLong()).isEqualTo(creationTime);

    val dataMeta = asObjectNode(meta.get(1));
    assertThat(dataMeta.get("id").asText()).isEqualTo(DATA.getId());
    assertThat(dataMeta.get("type").asText()).isEqualTo(DATA.getType());
    assertThat(dataMeta.get("url").asText()).isEqualTo(getIdUrl(DATA.getId()));
    assertThat(dataMeta.get("date").asLong()).isEqualTo(creationTime);
  }

  private static String getIdUrl(String id) {
    return format("%s/exports/%s", BASE_URL, id);
  }

}
