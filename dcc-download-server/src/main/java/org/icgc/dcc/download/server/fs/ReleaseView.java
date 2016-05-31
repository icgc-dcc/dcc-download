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

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.download.server.model.DownloadFileType.DIRECTORY;

import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.service.DownloadFileSystemService;

public class ReleaseView extends AbstractDownloadFileSystem {

  public ReleaseView(String rootDir, FileSystem fileSystem, @NonNull DownloadFileSystemService fsService) {
    super(rootDir, fileSystem, fsService);
  }

  public Collection<DownloadFile> listFiles(@NonNull String releasePath) {
    return null;
  }

  public Collection<DownloadFile> listRelease(@NonNull String releaseName) {
    val releaseFiles = HadoopUtils.lsAll(fileSystem, toHdfsPath(releaseName));
    val downloadFiles = releaseFiles.stream()
        .filter(file -> isDfsEntity(file) == false)
        .map(file -> convert2DownloadFile(file))
        .collect(toList());

    downloadFiles.add(createProjectsDir(releaseName));
    downloadFiles.add(createSummaryDir(releaseName));

    return downloadFiles;
  }

  public Collection<DownloadFile> listReleaseProjects(@NonNull String releaseName) {
    return null;
  }

  public Collection<DownloadFile> listReleaseSummary(@NonNull String releaseName) {
    return null;
  }

  private DownloadFile createSummaryDir(String releaseName) {
    return createDir(format("/%s/Summary", releaseName));
  }

  private DownloadFile createProjectsDir(String releaseName) {
    return createDir(format("/%s/Projects", releaseName));
  }

  private DownloadFile createDir(String path) {
    val dir = new DownloadFile();
    dir.setName(path);
    dir.setType(DIRECTORY);
    dir.setDate(currentTimeMillis());

    return dir;
  }

  private boolean isDfsEntity(Path file) {
    val fileName = file.getName();
    return fileName.equals(DATA_DIR) || fileName.equals(HEADERS_DIR);
  }

}
