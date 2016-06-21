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
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;

import java.io.File;
import java.util.Map;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.config.Properties;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.AbstractFsTest;
import org.icgc.dcc.download.server.utils.DownloadFsTests;
import org.junit.Test;

public class DownloadFilesReaderTest extends AbstractFsTest {

  FileSystem fileSystem = getDefaultLocalFileSystem();
  DownloadFilesReader downloadFilesReader;
  PathResolver pathResolver;

  @Override
  public void setUp() {
    super.setUp();
    val properties = new Properties.JobProperties();
    properties.setInputDir(workingDir.getAbsolutePath());
    pathResolver = new PathResolver(properties);
    downloadFilesReader = new DownloadFilesReader(fileSystem, pathResolver);
  }

  @Test
  public void testCreateReleaseCache() throws Exception {
    val releaseTable = downloadFilesReader.createReleaseCache(getReleasePath());
    assertThat(releaseTable.size()).isNotZero();
    assertDonor(releaseTable.row("DO001"), (short) 0, 8);
    assertDonor(releaseTable.row("DO002"), (short) 0, 8);
    assertDonor(releaseTable.row("DO003"), (short) 1, 6);
    assertDonor(releaseTable.row("DO004"), (short) 1, 6);
  }

  @Test
  public void testCreateProjectDonors() throws Exception {
    val projectDonors = downloadFilesReader.createProjectDonors(DownloadFsTests.createDonorFileTypesTable());
    assertThat(projectDonors.size()).isEqualTo(3);
    assertThat(projectDonors.get("TST1-CA")).containsOnly("DO001", "DO002");
    assertThat(projectDonors.get("TST2-CA")).containsOnly("DO003");

  }

  @Test
  public void testGetReleaseTimes() throws Exception {
    val releaseTimes = downloadFilesReader.getReleaseTimes();
    assertThat(releaseTimes).hasSize(1);
    assertThat(releaseTimes.get("release_21")).isEqualTo(getModificationTime("release_21"));
  }

  private Path getReleasePath() {
    val releaseDir = new File(workingDir, "release_21").getAbsolutePath();

    return new Path(releaseDir);
  }

  private void assertDonor(Map<DownloadDataType, DataTypeFile> row, Short expectedPartFile, int expectedDataTypes) {
    assertThat(row).hasSize(expectedDataTypes);
    for (val dataTypeFile : row.values()) {
      val partFiles = dataTypeFile.getPartFileIndices();
      assertThat(partFiles).hasSize(1);
      assertThat(partFiles.get(0)).isEqualTo(expectedPartFile);
    }
  }

  @Test
  public void testGetReleaseProjectDonors() throws Exception {
    val releaseProjects = downloadFilesReader.getReleaseProjectDonors();
    assertThat(releaseProjects.size()).isEqualTo(1);
    val projectDonors = releaseProjects.get("release_21");
    assertThat(projectDonors.size()).isEqualTo(4);
    assertThat(projectDonors.get("TST1-CA")).containsOnly("DO001", "DO002");
    assertThat(projectDonors.get("TST2-CA")).containsOnly("DO003", "DO004");
  }

}
