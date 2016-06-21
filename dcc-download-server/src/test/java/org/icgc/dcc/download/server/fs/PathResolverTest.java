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
package org.icgc.dcc.download.server.fs;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.config.Properties;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

@Slf4j
public class PathResolverTest {

  PathResolver pathResolver;

  @Before
  public void setUp() {
    val properties = new Properties.JobProperties();
    properties.setInputDir("/tmp");
    pathResolver = new PathResolver(properties);
  }

  @Test
  public void testGetPartFilePaths() throws Exception {
    val dataFile = new DataTypeFile("TST1-CA/DO002/donor", ImmutableList.of((short) 0, (short) 123), 123L);
    val paths = pathResolver.getPartFilePaths("release_21", dataFile);
    log.info("Paths: {}", paths);
    assertThat(paths).hasSize(2);
    assertThat(paths.get(0)).isEqualTo("/tmp/release_21/data/TST1-CA/DO002/donor/part-00000.gz");
    assertThat(paths.get(1)).isEqualTo("/tmp/release_21/data/TST1-CA/DO002/donor/part-00123.gz");
  }

  @Test
  public void testToHdfsPath() throws Exception {
    assertThat(pathResolver.toHdfsPath("/release_21").toString()).isEqualTo("/tmp/release_21");
  }

  @Test
  public void testGetDataFilePath() throws Exception {
    assertThat(pathResolver.getDataFilePath("file:///tmp/release_21/data/TST1-CA/DO002/donor/part-00123.gz"))
        .isEqualTo("TST1-CA/DO002/donor");
  }

  @Test
  public void testGetPartFileIndex() throws Exception {
    assertThat(pathResolver.getPartFileIndex("part-00123.gz")).isEqualTo((short) 123);
  }

}
