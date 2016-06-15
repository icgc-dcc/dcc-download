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
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;
import static org.mockito.Mockito.mock;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Slf4j
public class GzipStreamerTest extends AbstractTest {

  String rootDir;
  GzipStreamer gzipStreamer;
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
    val output = new BufferedOutputStream(new FileOutputStream(testFile));

    gzipStreamer =
        new GzipStreamer(getDefaultLocalFileSystem(), getDownloadFiles(), getDownloadSizes(), getHeaders(), output);

    try {
      assertThat(gzipStreamer.hasNext()).isTrue();
      assertThat(gzipStreamer.getNextEntryName()).isEqualTo("donor.gz");
      assertThat(gzipStreamer.getNextEntryLength()).isEqualTo(706L);

      gzipStreamer.streamEntry();
      assertThat(gzipStreamer.hasNext()).isFalse();
    } finally {
      output.close();
    }

    assertDonorTestFile();
  }

  @Test
  public void testRead_multipleTypes() throws Exception {
    val output = new BufferedOutputStream(new FileOutputStream(testFile));

    gzipStreamer = new GzipStreamer(getDefaultLocalFileSystem(), getMultipleDownloadFiles(),
        getMultipleDownloadSizes(), getMultipleHeaders(), output);

    try {
      assertThat(gzipStreamer.hasNext()).isTrue();
      assertThat(gzipStreamer.getNextEntryName()).isEqualTo("donor.gz");
      assertThat(gzipStreamer.getNextEntryLength()).isEqualTo(706L);

      gzipStreamer.streamEntry();

      assertThat(gzipStreamer.hasNext()).isTrue();
      assertThat(gzipStreamer.getNextEntryName()).isEqualTo("sample.gz");
      assertThat(gzipStreamer.getNextEntryLength()).isEqualTo(314L);

      gzipStreamer.streamEntry();

      assertThat(gzipStreamer.hasNext()).isFalse();
    } finally {
      output.close();
    }

    assertDonorSampleTestFile();
  }

  @Test
  public void testGetNextEntryName() throws Exception {
    gzipStreamer = new GzipStreamer(
        mock(FileSystem.class),
        singletonList(new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/ssm_open", of("part-00000.gz"), 1)),
        emptyMap(),
        getHeaders(),
        mock(OutputStream.class));
    assertThat(gzipStreamer.getName()).isEqualTo("simple_somatic_mutation.open.tsv.gz");
  }

  private void assertDonorTestFile() throws Exception {
    val lines = readTestFile();
    assertThat(lines).hasSize(5);
    assertDonor(lines);
  }

  private void assertDonorSampleTestFile() throws Exception {
    val lines = readTestFile();
    assertThat(lines).hasSize(9);
    assertDonor(lines);
    assertSample(lines);
  }

  private List<String> readTestFile() throws Exception {
    @Cleanup
    val gzipInput = new GZIPInputStream(new FileInputStream(testFile));
    val lines = readInMemory(gzipInput);

    lines.stream()
        .forEach(line -> log.info(line));

    return lines;
  }

  private static void assertDonor(@NonNull List<String> lines) {
    assertDonorLine(lines.get(0), "icgc_donor_id");
    assertDonorLine(lines.get(1), "DO001");
    assertDonorLine(lines.get(2), "DO002");
    assertDonorLine(lines.get(3), "DO003");
    assertDonorLine(lines.get(4), "DO004");
  }

  private static void assertSample(@NonNull List<String> lines) {
    assertSampleLine(lines.get(5), "icgc_sample_id");
    assertSampleLine(lines.get(6), "SA000001");
    assertSampleLine(lines.get(7), "SA000002");
    assertSampleLine(lines.get(8), "SA000002");
  }

  private static Map<DownloadDataType, Long> getDownloadSizes() {
    return Collections.singletonMap(DONOR, 472L);
  }

  private static Map<DownloadDataType, Long> getMultipleDownloadSizes() {
    return ImmutableMap.of(
        DONOR, 472L,
        SAMPLE, 194L);
  }

  private static void assertDonorLine(String line, String firstPart) {
    assertLine(line, firstPart, 21);
  }

  private static void assertSampleLine(String line, String firstPart) {
    assertLine(line, firstPart, 11);
  }

  private static void assertLine(String line, String firstPart, int colNum) {
    val parts = Splitters.TAB.splitToList(line);
    assertThat(parts).hasSize(colNum);
    assertThat(parts.get(0)).isEqualTo(firstPart);
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

  private Map<DownloadDataType, String> getHeaders() {
    val headerPath = rootDir + "/release_21/headers/donor.tsv.gz";

    return Collections.singletonMap(DONOR, headerPath);
  }

  private Map<DownloadDataType, String> getMultipleHeaders() {
    val headerPath = rootDir + "/release_21/headers/";

    return ImmutableMap.of(
        DONOR, headerPath + "donor.tsv.gz",
        SAMPLE, headerPath + "sample.tsv.gz");
  }

  private List<DataTypeFile> getDownloadFiles() {
    return of(
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/donor", of("part-00000.gz"), 130),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO002/donor", of("part-00000.gz"), 111),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO003/donor", of("part-00001.gz"), 119),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO004/donor", of("part-00001.gz"), 112));
  }

  private List<DataTypeFile> getMultipleDownloadFiles() {
    return of(
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/donor", of("part-00000.gz"), 130),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO002/donor", of("part-00000.gz"), 111),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO003/donor", of("part-00001.gz"), 119),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO004/donor", of("part-00001.gz"), 112),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/sample", of("part-00000.gz"), 97),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO002/sample", of("part-00000.gz"), 97));
  }

}
