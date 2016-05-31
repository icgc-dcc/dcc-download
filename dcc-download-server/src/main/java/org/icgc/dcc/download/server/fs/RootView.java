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

import static org.icgc.dcc.download.server.model.DownloadFileType.DIRECTORY;

import java.util.Collection;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DownloadFile;

import com.google.common.collect.ImmutableList;

@Slf4j
public class RootView extends AbstractDownloadFileSystem {

  public RootView(@NonNull String rootDir, @NonNull FileSystem fileSystem) {
    super(rootDir, fileSystem);
  }

  /**
   * Lists releases in the root directory.
   */
  public Collection<DownloadFile> listReleases() {
    val allFiles = HadoopUtils.lsAll(fileSystem, rootPath);
    log.debug("[/]: {}", allFiles);

    val dfsFilesBuilder = ImmutableList.<DownloadFile> builder();
    for (val file : allFiles) {
      val rootFile = convert2DownloadFile(file);
      dfsFilesBuilder.add(rootFile);
      if (isCurrentReleaseFile(rootFile)) {
        dfsFilesBuilder.add(addCurrentLinkFile(rootFile));
      }
    }

    val dfsFiles = dfsFilesBuilder.build();
    log.debug("DFS files: {}", dfsFiles);

    return dfsFiles;
  }

  private static DownloadFile addCurrentLinkFile(DownloadFile file) {
    val currentLinkFile = new DownloadFile();
    currentLinkFile.setDate(file.getDate());
    currentLinkFile.setName(CURRENT_PATH);
    currentLinkFile.setType(DIRECTORY);

    return currentLinkFile;
  }

}
