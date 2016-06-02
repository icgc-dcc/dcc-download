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
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

@Slf4j
public class GzipStreamerTest extends AbstractTest {

  String rootDir;
  GzipStreamer gzipStreamer;

  @Before
  @Override
  public void setUp() {
    this.rootDir = new File(INPUT_TEST_FIXTURES_DIR).getAbsolutePath();
  }

  @Test
  public void testRead() throws Exception {
    gzipStreamer = new GzipStreamer(getDefaultLocalFileSystem(), getDownloadFiles(), emptyMap(), getHeaders());
    @Cleanup
    val gzipInput = new GZIPInputStream(gzipStreamer);
    val lines = readInMemory(gzipInput);

    lines.stream()
        .forEach(line -> log.info(line));

    assertThat(lines).hasSize(5);
    assertDonorLine(lines.get(0), "icgc_donor_id");
    assertDonorLine(lines.get(1), "DO001");
    assertDonorLine(lines.get(2), "DO002");
    assertDonorLine(lines.get(3), "DO003");
    assertDonorLine(lines.get(4), "DO004");
  }

  private void assertDonorLine(String line, String firstPart) {
    val parts = Splitters.TAB.splitToList(line);
    assertThat(parts).hasSize(21);
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

  private List<DataTypeFile> getDownloadFiles() {
    return of(
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO001/donor", of("part-00000.gz"), 0),
        new DataTypeFile(rootDir + "/release_21/data/TST1-CA/DO002/donor", of("part-00000.gz"), 0),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO003/donor", of("part-00001.gz"), 0),
        new DataTypeFile(rootDir + "/release_21/data/TST2-CA/DO004/donor", of("part-00001.gz"), 0));
  }

}
