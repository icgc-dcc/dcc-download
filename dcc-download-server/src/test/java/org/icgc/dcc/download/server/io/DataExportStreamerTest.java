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
import static org.icgc.dcc.download.server.model.ExportEntity.DATA;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.common.test.file.FileTests;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class DataExportStreamerTest extends AbstractTest {

  DataExportStreamer streamer;
  FileSystem fileSystem;

  @Override
  @Before
  public void setUp() {
    fileSystem = FileSystems.getDefaultLocalFileSystem();
  }

  @Test
  public void testStream() throws Exception {
    val testFile = FileTests.getTempFile();
    val outStream = new BufferedOutputStream(new FileOutputStream(testFile));

    streamer = new DataExportStreamer(new Path(INPUT_TEST_FIXTURES_DIR + "/release_21"), fileSystem, outStream);
    streamer.stream();
    streamer.close();

    assertThat(streamer.getName()).isEqualTo(DATA.getId());

    @Cleanup
    val tarIn = new TarArchiveInputStream(new FileInputStream(testFile));
    int filesCount = 0;
    TarArchiveEntry tarEntry = null;
    while ((tarEntry = tarIn.getNextTarEntry()) != null) {
      log.info("Entry name: {}", tarEntry.getName());
      assertThat(tarEntry.getSize()).isGreaterThan(0);

      filesCount++;
    }
    assertThat(filesCount).isEqualTo(50);
  }
}
