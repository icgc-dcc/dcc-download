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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;

import java.io.File;
import java.util.Map;

import lombok.val;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.AbstractFsTest;
import org.junit.Test;

public class DownloadFileSystemServiceTest extends AbstractFsTest {

  @Test
  public void testCreateReleaseCache() throws Exception {
    val release21Dir = new File(workingDir, "release_21").getAbsolutePath();
    val releaseTable =
        DownloadFileSystemService.createReleaseCache(new Path(release21Dir), getDefaultLocalFileSystem());
    assertThat(releaseTable.size()).isNotZero();
    assertDonor(releaseTable.row("DO001"), "part-00000.gz", 8);
    assertDonor(releaseTable.row("DO002"), "part-00000.gz", 8);
    assertDonor(releaseTable.row("DO003"), "part-00001.gz", 6);
    assertDonor(releaseTable.row("DO004"), "part-00001.gz", 6);
  }

  private void assertDonor(Map<DownloadDataType, DataTypeFile> row, String expectedPartFile, int expectedDataTypes) {
    assertThat(row).hasSize(expectedDataTypes);
    for (val dataTypeFile : row.values()) {
      val partFiles = dataTypeFile.getPartFiles();
      assertThat(partFiles).hasSize(1);
      assertThat(partFiles.get(0)).isEqualTo(expectedPartFile);
    }
  }

}
