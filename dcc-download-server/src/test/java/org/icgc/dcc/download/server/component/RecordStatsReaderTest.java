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
package org.icgc.dcc.download.server.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_EXPOSURE;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_FAMILY;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR_THERAPY;
import static org.icgc.dcc.download.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.download.core.model.DownloadDataType.SGV_CONTROLLED;
import static org.icgc.dcc.download.core.model.DownloadDataType.SPECIMEN;
import static org.icgc.dcc.download.core.model.DownloadDataType.SSM_CONTROLLED;
import static org.icgc.dcc.download.core.model.DownloadDataType.SSM_OPEN;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.hadoop.fs.FileSystems;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.server.config.Properties;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

@Slf4j
public class RecordStatsReaderTest extends AbstractTest {

  RecordStatsReader reader;

  @Override
  @Before
  public void setUp() {
    super.setUp();
    prepareInput();
    val fs = FileSystems.getDefaultLocalFileSystem();
    val jobProps = new Properties.JobProperties();
    jobProps.setInputDir(workingDir.getAbsolutePath());
    val downloadProps = new Properties.DownloadServerProperties();
    reader = new RecordStatsReader(jobProps, downloadProps, fs);
  }

  @Test
  public void testReadStatsTable() throws Exception {
    val stats = reader.readStatsTable();
    log.info("{}", stats);
    assertThat(stats.rowKeySet().size()).isEqualTo(4);
    val donor1 = stats.row("DO001");
    assertThat(donor1).isEqualTo(ImmutableMap.<DownloadDataType, Long> builder()
        .put(DONOR, 1L)
        .put(DONOR_EXPOSURE, 1L)
        .put(DONOR_FAMILY, 1L)
        .put(SPECIMEN, 1L)
        .put(SSM_CONTROLLED, 341L)
        .put(SSM_OPEN, 341L)
        .put(SGV_CONTROLLED, 3763280L)
        .put(DONOR_THERAPY, 1L)
        .put(SAMPLE, 2L)
        .build());
  }

}
