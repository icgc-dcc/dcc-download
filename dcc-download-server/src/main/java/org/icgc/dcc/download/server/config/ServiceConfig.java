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
package org.icgc.dcc.download.server.config;

import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.fs.DownloadFileSystem;
import org.icgc.dcc.download.server.fs.DownloadFilesReader;
import org.icgc.dcc.download.server.fs.PathResolver;
import org.icgc.dcc.download.server.fs.ReleaseView;
import org.icgc.dcc.download.server.fs.RootView;
import org.icgc.dcc.download.server.repository.DataFilesRepository;
import org.icgc.dcc.download.server.repository.JobRepository;
import org.icgc.dcc.download.server.service.ArchiveDownloadService;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfig {

  @Autowired
  private JobProperties jobProperties;
  @Autowired
  private FileSystem fileSystem;
  @Autowired
  private PathResolver pathResolver;

  @Bean
  public ArchiveDownloadService archiveDownloadService(
      FileSystemService fileSystemService,
      JobRepository jobRepository,
      DataFilesRepository dataFilesRepository) {
    return new ArchiveDownloadService(
        getRootPath(),
        fileSystemService,
        fileSystem,
        jobRepository,
        dataFilesRepository,
        pathResolver);
  }

  @Bean
  public DownloadFileSystem downloadFileSystem(FileSystemService fileSystemService) {
    val rootView = new RootView(fileSystem, fileSystemService, pathResolver);
    val releaseView = new ReleaseView(fileSystem, fileSystemService, pathResolver);

    return new DownloadFileSystem(rootView, releaseView);
  }

  @Bean
  public FileSystemService fileSystemService() {
    return new FileSystemService(downloadFilesReader());
  }

  private DownloadFilesReader downloadFilesReader() {
    return new DownloadFilesReader(fileSystem, pathResolver);
  }

  private Path getRootPath() {
    return new Path(jobProperties.getInputDir());
  }

}
