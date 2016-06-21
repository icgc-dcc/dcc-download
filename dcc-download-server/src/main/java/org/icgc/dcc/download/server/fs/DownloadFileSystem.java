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

import java.util.Collection;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.server.utils.DfsPaths;

@Slf4j
@RequiredArgsConstructor
public class DownloadFileSystem {

  @NonNull
  private final RootView rootView;
  @NonNull
  private final ReleaseView releaseView;

  public Collection<DownloadFile> listFiles(@NonNull String path) {
    log.debug("Listing files for path '{}'...", path);
    DfsPaths.validatePath(path);

    // "/"
    if ("/".equals(path)) {
      return rootView.listReleases();
    }

    val pathParts = Splitters.PATH.splitToList(path);
    val releaseName = pathParts.get(1);
    // "/release_21"
    if (pathParts.size() == 2) {
      return releaseView.listRelease(releaseName);
    }

    // "/release_21/{Projects|Summary}"
    if (pathParts.size() == 3) {
      val dir = pathParts.get(2);
      switch (dir) {
      case "Projects":
        return releaseView.listReleaseProjects(releaseName);
      case "Summary":
        return releaseView.listReleaseSummary(releaseName);
      default:
        throw new IllegalArgumentException(format("Malformed path '%s'", path));
      }
    }

    // /release_21/Projects/TST1-CA
    if (pathParts.size() == 4) {
      val project = pathParts.get(3);

      return releaseView.listProject(releaseName, project);
    }

    throw new IllegalArgumentException(format("Malformed path '%s'", path));
  }

}
