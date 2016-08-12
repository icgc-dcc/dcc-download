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

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.core.model.DownloadFileType.DIRECTORY;
import static org.icgc.dcc.download.server.utils.Releases.getReleaseNumber;
import static org.icgc.dcc.download.server.utils.Releases.isLegacyReleaseName;

import java.util.List;

import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.server.service.FileSystemService;

import com.google.common.collect.Lists;

public class RootView extends AbstractFileSystemView {

  public RootView(@NonNull FileSystem fileSystem, @NonNull FileSystemService fsService,
      @NonNull PathResolver pathResolver) {
    super(fileSystem, fsService, pathResolver);
  }

  /**
   * Lists releases in the root directory.
   */
  public List<DownloadFile> listReleases() {
    val allFiles = HadoopUtils.lsAll(fileSystem, pathResolver.getRootPath());
    val dfsFiles = Lists.<DownloadFile> newArrayList();
    for (val file : allFiles) {
      if (isNextReleaseDir(file)) {
        continue;
      }

      val rootFile = convert2DownloadFile(file, false);
      dfsFiles.add(rootFile);
      if (isCurrentReleaseFile(rootFile)) {
        dfsFiles.add(addCurrentLinkFile(rootFile));
      }
    }

    return dfsFiles.stream()
        .sorted()
        .collect(toImmutableList());
  }

  private boolean isNextReleaseDir(Path file) {
    val releaseName = file.getName();
    if (isLegacyReleaseName(releaseName)) {
      return false;
    }

    val currentRelease = fsService.getCurrentRelease();
    val releaseNumber = getReleaseNumber(releaseName);
    val currentReleaseNumber = getReleaseNumber(currentRelease);

    return releaseNumber > currentReleaseNumber;
  }

  private static DownloadFile addCurrentLinkFile(DownloadFile file) {
    val currentLinkFile = new DownloadFile();
    currentLinkFile.setDate(file.getDate());
    currentLinkFile.setName(CURRENT_PATH);
    currentLinkFile.setType(DIRECTORY);

    return currentLinkFile;
  }

}
