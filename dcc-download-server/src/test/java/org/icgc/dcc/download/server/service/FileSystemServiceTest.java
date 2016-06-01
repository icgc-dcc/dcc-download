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

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.download.server.utils.DownloadFsTests.createDonorFileTypesTable;
import static org.icgc.dcc.download.server.utils.DownloadFsTests.createProjectDonors;
import static org.mockito.Mockito.when;
import lombok.val;

import org.icgc.dcc.download.server.fs.DownloadFilesReader;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemServiceTest {

  @Mock
  DownloadFilesReader reader;

  FileSystemService service;

  @Before
  public void setUp() {
    when(reader.getReleaseTimes()).thenReturn(ImmutableMap.of("release_21", 123L));
    when(reader.getReleaseProjectDonors()).thenReturn(ImmutableMap.of("release_21", createProjectDonors()));
    when(reader.getReleaseDonorFileTypes()).thenReturn(ImmutableMap.of("release_21", createDonorFileTypesTable()));
    service = new FileSystemService(reader);
  }

  @Test
  public void testGetReleaseProjects() throws Exception {
    val projects = service.getReleaseProjects("release_21");
    assertThat(projects).containsExactly("TST1-CA", "TST2-CA");
  }

  @Test
  public void testGetReleaseDate() throws Exception {
    assertThat(service.getReleaseDate("release_21")).isEqualTo(123);
  }

  @Test
  public void testGetClinicalSizes() throws Exception {
    val clinicalSizes = service.getClinicalSizes("release_21");
    assertThat(clinicalSizes).isEqualTo(ImmutableMap.of(DONOR, 8L, SAMPLE, 2L));
  }

  @Test
  public void testGetProjectSizes() throws Exception {
    assertThat(service.getProjectSizes("release_21", "TST1-CA")).isEqualTo(ImmutableMap.of(DONOR, 4L, SAMPLE, 2L));
    assertThat(service.getProjectSizes("release_21", "TST2-CA")).isEqualTo(ImmutableMap.of(DONOR, 4L));
  }

  @Test
  public void testGetDataTypeFiles() throws Exception {
    val files = service.getDataTypeFiles("release_21", ImmutableList.of("DO002", "DO001"),
        ImmutableList.of(SAMPLE, DONOR));
    assertThat(files).hasSize(3);
    assertThat(files.get(0)).isEqualTo(
        new DataTypeFile("/somepath/release_21/TST1-CA/DO001/sample", of("part-00000.gz"), 2));
    assertThat(files.get(1)).isEqualTo(
        new DataTypeFile("/somepath/release_21/TST1-CA/DO001/donor", of("part-00000.gz"), 1));
    assertThat(files.get(2)).isEqualTo(
        new DataTypeFile("/somepath/release_21/TST1-CA/DO002/donor", of("part-00000.gz", "part-00001.gz"), 3));

  }
}
