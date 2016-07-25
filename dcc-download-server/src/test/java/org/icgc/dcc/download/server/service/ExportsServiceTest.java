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
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;
import static org.icgc.dcc.download.server.model.Export.DATA;
import static org.icgc.dcc.download.server.model.Export.RELEASE_CONTROLLED;
import static org.icgc.dcc.download.server.model.Export.RELEASE_OPEN;
import static org.icgc.dcc.download.server.model.Export.REPOSITORY;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;
import static org.icgc.dcc.download.test.io.TestFiles.copyDirectory;

import java.io.File;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.server.model.Export;
import org.icgc.dcc.download.server.model.ExportFile;
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
    copyDirectory(new File(TEST_FIXTURES_DIR, "es_export"), new File(workingDir, "es_export"));
    val repoFile = new File(workingDir, REPOSITORY.getId());
    repoFile.createNewFile();

    val metaFiles = service.getMetadata(BASE_URL).getFiles();

    log.info("{}", metaFiles);
    assertThat(metaFiles).hasSize(3);
    val creationTime = getFileStatus(fileSystem, new Path(workingDir.getAbsolutePath())).getModificationTime();
    val iterator = metaFiles.iterator();

    val repoMeta = iterator.next();
    verifyNonReleaseExportFile(repoMeta, REPOSITORY, creationTime);

    val dataMeta = iterator.next();
    verifyNonReleaseExportFile(dataMeta, DATA, creationTime);

    val releaseMeta = iterator.next();
    verifyReleaseOpen(releaseMeta, creationTime);
  }

  @Test
  public void testGetControlledMetadata() throws Exception {
    copyDirectory(new File(TEST_FIXTURES_DIR, "es_export"), new File(workingDir, "es_export"));
    val repoFile = new File(workingDir, REPOSITORY.getId());
    repoFile.createNewFile();

    val metaFiles = service.getControlledMetadata(BASE_URL).getFiles();

    log.info("{}", metaFiles);
    assertThat(metaFiles).hasSize(4);
    val creationTime = getFileStatus(fileSystem, new Path(workingDir.getAbsolutePath())).getModificationTime();
    val iterator = metaFiles.iterator();

    val repoMeta = iterator.next();
    verifyNonReleaseExportFile(repoMeta, REPOSITORY, creationTime);

    val dataMeta = iterator.next();
    verifyNonReleaseExportFile(dataMeta, DATA, creationTime);

    val releaseOpenMeta = iterator.next();
    verifyReleaseOpen(releaseOpenMeta, creationTime);

    val releaseControlledMeta = iterator.next();
    verifyReleaseControlled(releaseControlledMeta, creationTime);
  }

  private static void verifyReleaseOpen(ExportFile file, long creationTime) {
    assertThat(file.getId()).isEqualTo("release21.open.tar");
    assertThat(file.getType()).isEqualTo(RELEASE_OPEN.getType());
    assertThat(file.getUrl()).isEqualTo(format("%s/exports/%s", BASE_URL, RELEASE_OPEN.getId(21)));
    assertThat(file.getDate()).isEqualTo(creationTime);
  }

  private static void verifyReleaseControlled(ExportFile file, long creationTime) {
    assertThat(file.getId()).isEqualTo("release21.controlled.tar");
    assertThat(file.getType()).isEqualTo(RELEASE_CONTROLLED.getType());
    assertThat(file.getUrl()).isEqualTo(format("%s/exports/%s", BASE_URL, RELEASE_CONTROLLED.getId(21)));
    assertThat(file.getDate()).isEqualTo(creationTime);
  }

  private static void verifyNonReleaseExportFile(ExportFile file, Export expectedExport, long creationTime) {
    assertThat(file.getId()).isEqualTo(expectedExport.getId());
    assertThat(file.getType()).isEqualTo(expectedExport.getType());
    assertThat(file.getUrl()).isEqualTo(getIdUrl(expectedExport.getId()));
    assertThat(file.getDate()).isEqualTo(creationTime);
  }

  private static String getIdUrl(String id) {
    return format("%s/exports/%s", BASE_URL, id);
  }

}
