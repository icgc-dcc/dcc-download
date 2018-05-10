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
package org.icgc.dcc.download.server.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.server.model.Export.DATA_CONTROLLED;
import static org.icgc.dcc.download.server.model.Export.DATA_OPEN;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Optional;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.common.test.file.FileTests;
import org.icgc.dcc.download.server.model.Export;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataExportStreamerTest extends AbstractTest {

  DataExportStreamer streamer;
  FileSystem fileSystem;
  File testFile;

  @Override
  @Before
  public void setUp() {
    fileSystem = FileSystems.getDefaultLocalFileSystem();
    testFile = FileTests.getTempFile();
  }

  @Test
  public void testStreamOpen() throws Exception {
    testStreamOpen(Optional.empty(), 55);
  }

  @Test
  public void testStreamOpenWithProject() throws Exception {
    testStreamOpen(Optional.of("TST1-CA"), 39);
  }

  @Test
  public void testStreamControlled() throws Exception {
    testControlledStream(Optional.empty(), 58);
  }

  @Test
  public void testStreamControlledWithProject() throws Exception {
    testControlledStream(Optional.of("TST2-CA"), 38);
  }

  private void testStreamOpen(Optional<String> project, int expectedFilesCount) throws Exception {
    streamer = getDataStreamer(DATA_OPEN, project);
    streamer.stream();
    streamer.close();

    assertThat(streamer.getName()).isEqualTo(DATA_OPEN.getId());

    @Cleanup
    val tarIn = new TarArchiveInputStream(new FileInputStream(testFile));
    int filesCount = 0;
    TarArchiveEntry tarEntry = null;
    while ((tarEntry = tarIn.getNextTarEntry()) != null) {
      val fileName = tarEntry.getName();
      log.info("Entry name: {}", fileName);
      assertThat(tarEntry.getSize()).isGreaterThan(0);
      assertThat(fileName.contains("controlled")).isFalse();

      filesCount++;
    }
    assertThat(filesCount).isEqualTo(expectedFilesCount);
  }

  private void testControlledStream(Optional<String> project, int expectedFilesCount) throws Exception {
    streamer = getDataStreamer(DATA_CONTROLLED, project);
    streamer.stream();
    streamer.close();

    assertThat(streamer.getName()).isEqualTo(DATA_CONTROLLED.getId());

    @Cleanup
    val tarIn = new TarArchiveInputStream(new FileInputStream(testFile));
    int filesCount = 0;
    TarArchiveEntry tarEntry = null;
    boolean hasControlled = false;
    while ((tarEntry = tarIn.getNextTarEntry()) != null) {
      val fileName = tarEntry.getName();
      log.info("Entry name: {}", fileName);
      assertThat(tarEntry.getSize()).isGreaterThan(0);
      if (fileName.contains("controlled")) {
        hasControlled = true;
      }

      filesCount++;
    }
    assertThat(filesCount).isEqualTo(expectedFilesCount);
    assertThat(hasControlled).isTrue();
  }

  @SneakyThrows
  private DataExportStreamer getDataStreamer(Export export, Optional<String> project) {
    val outStream = new BufferedOutputStream(new FileOutputStream(testFile));

    return new DataExportStreamer(
        new Path(INPUT_TEST_FIXTURES_DIR + "/release_21"),
        export,
        fileSystem,
        outStream,
        project);
  }

}
