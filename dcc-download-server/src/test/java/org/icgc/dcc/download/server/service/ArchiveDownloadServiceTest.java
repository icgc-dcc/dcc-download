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
import static org.mockito.Mockito.when;

import java.io.OutputStream;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.server.fs.PathResolver;
import org.icgc.dcc.download.server.repository.DataFilesRepository;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ArchiveDownloadServiceTest {

  Path rootPath = new Path("/tmp");
  @Mock
  FileSystemService fileSystemService;
  @Mock
  FileSystem fileSystem;
  @Mock
  JobRepository jobRepository;
  @Mock
  DataFilesRepository dataFilesRepository;
  @Mock
  PathResolver pathResolver;

  @Mock
  OutputStream output;

  ArchiveDownloadService service;

  @Before
  public void setUp() {
    service =
        new ArchiveDownloadService(rootPath, fileSystemService, fileSystem, jobRepository, dataFilesRepository,
            pathResolver);
  }

  @Test
  public void testGetStaticArchiveStreamer() throws Exception {
    when(fileSystemService.isLegacyRelease("release_21")).thenReturn(false);
    when(fileSystemService.getCurrentRelease()).thenReturn("release_21");
    when(fileSystem.exists(new Path("/tmp/release_21/projects_files/README.txt"))).thenReturn(true);

    val streamerOpt = service.getStaticArchiveStreamer("/release_21/Projects/README.txt", output);
    assertThat(streamerOpt.isPresent()).isTrue();
    val streamer = streamerOpt.get();
    assertThat(streamer.getName()).isEqualTo("README.txt");
  }

}
