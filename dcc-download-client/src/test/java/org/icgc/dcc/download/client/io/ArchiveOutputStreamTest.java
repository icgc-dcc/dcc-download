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
package org.icgc.dcc.download.client.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

@Slf4j
public class ArchiveOutputStreamTest extends AbstractTest {

  private static final String DOWNLOAD_ID = "zzz123";
  private static final String OUT_FILE = "out";

  ArchiveOutputStream archiveOutputStream;

  @Before
  @Override
  @SneakyThrows
  public void setUp() {
    super.setUp();

    val destDir = new File(workingDir, DOWNLOAD_ID);
    prepareInput(destDir);

    val downloadPath = new Path(workingDir.getAbsolutePath());
    val fileSystem = FileSystem.getLocal(new Configuration());
    archiveOutputStream = new ArchiveOutputStream(downloadPath, fileSystem);
  }

  @Test
  public void testStreamArchiveInTarGz() throws Exception {
    val outputStream = getOutputStream();
    archiveOutputStream.streamArchiveInTarGz(outputStream, DOWNLOAD_ID,
        ImmutableList.of(DownloadDataType.DONOR, DownloadDataType.SPECIMEN));

    @Cleanup
    val tarStream = readArchiveStream();
    TarArchiveEntry entry = null;
    int entriesNum = 0;
    while ((entry = tarStream.getNextTarEntry()) != null) {
      assertThat(entry.getSize()).isGreaterThan(100L);
      switch (entriesNum++) {
      case 0:
        assertThat(entry.getName()).isEqualTo("donor.tsv.gz");
        break;
      case 1:
        assertThat(entry.getName()).isEqualTo("specimen.tsv.gz");
        break;
      default:
        fail("Extra lines");
      }
    }

  }

  @Test
  public void testStreamArchiveInGz() throws Exception {
    val outputStream = getOutputStream();
    archiveOutputStream.streamArchiveInGz(outputStream, DOWNLOAD_ID, DownloadDataType.DONOR);
    outputStream.close();

    val inputStream = getGzipReader();
    String line = null;
    int lineNum = 0;
    while ((line = inputStream.readLine()) != null) {
      log.info(line);
      switch (lineNum++) {
      case 0:
        assertThat(line).startsWith("icgc_donor_id");
        break;
      case 1:
        assertThat(line).startsWith("DO002");
        break;
      case 2:
        assertThat(line).startsWith("DO001");
        break;
      default:
        fail("Extra lines");
      }
    }

  }

  @SneakyThrows
  private OutputStream getOutputStream() {
    return new FileOutputStream(new File(workingDir, OUT_FILE));
  }

  @SneakyThrows
  private BufferedReader getGzipReader() {
    return new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(workingDir, OUT_FILE)))));
  }

  @SneakyThrows
  private TarArchiveInputStream readArchiveStream() {
    val outFile = new File(workingDir, OUT_FILE);

    return new TarArchiveInputStream(new FileInputStream(outFile));
  }

}
