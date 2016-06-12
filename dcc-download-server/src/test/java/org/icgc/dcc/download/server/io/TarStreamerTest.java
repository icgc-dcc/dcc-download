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

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class TarStreamerTest extends AbstractTest {

  String rootDir;
  TarStreamer tarStreamer;
  File testFile;

  @Before
  @Override
  @SneakyThrows
  public void setUp() {
    this.rootDir = new File(INPUT_TEST_FIXTURES_DIR).getAbsolutePath();
    testFile = File.createTempFile("gzip", "test", workingDir);
  }

  @Test
  public void testRead() throws Exception {
    val outStream = new BufferedOutputStream(new FileOutputStream(testFile));
    val tarOutput = createTarOutputStream(outStream);
    val gzipStremer = createGzipStreamer(tarOutput);

    tarStreamer = new TarStreamer(tarOutput, gzipStremer);
    tarStreamer.streamArchive();
    tarOutput.close();
    verifyTar();
  }

  private void verifyTar() throws Exception {
    @Cleanup
    val tarIn = new TarArchiveInputStream(new FileInputStream(testFile));
    val donorEntry = tarIn.getNextTarEntry();
    assertThat(donorEntry.getName()).isEqualTo("donor.gz");
    assertThat(donorEntry.getSize()).isEqualTo(364);

    val sampleEntry = tarIn.getNextTarEntry();
    assertThat(sampleEntry.getName()).isEqualTo("sample.gz");
    assertThat(sampleEntry.getSize()).isEqualTo(185);
  }

  private GzipStreamer createGzipStreamer(OutputStream output) {
    return new GzipStreamer(getDefaultLocalFileSystem(), getDownloadFiles(), getDownloadSizes(), getHeaders(), output);
  }

  @SneakyThrows
  private List<String> readInMemory(InputStream in) {
    val reader = new BufferedReader(new InputStreamReader(in));
    val lines = ImmutableList.<String> builder();
    String line = null;
    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }

    return lines.build();
  }

  private List<DataTypeFile> getDownloadFiles() {
    return of(
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/donor", of("part-00000.gz"), 130),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO002/sample", of("part-00000.gz"), 65));
  }

  private static Map<DownloadDataType, Long> getDownloadSizes() {
    return ImmutableMap.of(
        DONOR, 130L,
        SAMPLE, 65L);
  }

  private Map<DownloadDataType, String> getHeaders() {
    val headerPath = rootDir + "/release_21/headers/";

    return ImmutableMap.of(
        DONOR, headerPath + "donor.tsv.gz",
        SAMPLE, headerPath + "sample.tsv.gz");
  }

  private static TarArchiveOutputStream createTarOutputStream(OutputStream out) {
    log.debug("Creating tar output stream...");
    val tarOut = new TarArchiveOutputStream(out);
    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOut;
  }

}
