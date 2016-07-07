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
import static org.icgc.dcc.download.server.utils.DfsPaths.getLegacyRelease;
import static org.icgc.dcc.download.server.utils.DfsPaths.getProjectsPath;
import static org.icgc.dcc.download.server.utils.DfsPaths.getSummaryPath;
import static org.icgc.dcc.download.server.utils.DfsPaths.validatePath;
import static org.icgc.dcc.download.server.utils.Responses.throwBadRequestException;

import java.util.Collection;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.server.endpoint.BadRequestException;
import org.icgc.dcc.download.server.utils.DfsPaths;

@Slf4j
@RequiredArgsConstructor
public class DownloadFileSystem {

  private static final int ONE_LEVEL_PATH_SIZE = 2;
  private static final int TWO_LEVEL_PATH_SIZE = 3;
  private static final int THREE_LEVEL_PATH_SIZE = 4;

  @NonNull
  private final RootView rootView;
  @NonNull
  private final ReleaseView releaseView;

  /**
   * Non-legacy releases.
   */
  @NonNull
  private final Collection<String> releases;

  public Collection<DownloadFile> listFiles(@NonNull String path) {
    log.info("Listing files for path '{}'...", path);

    // "/"
    if ("/".equals(path)) {
      log.debug("Listing releases...");
      return rootView.listReleases();
    }

    val pathParts = Splitters.PATH.splitToList(path);
    if (isLegacyRelease(getLegacyRelease(pathParts))) {
      log.info("'{}' is a legacy path. Listing path contents as is.", path);

      return releaseView.listLegacy(path);
    }

    validatePath(path);

    val releaseName = pathParts.get(1);
    // "/release_21"
    if (pathParts.size() == ONE_LEVEL_PATH_SIZE) {
      log.debug("Listing release '{}'", releaseName);
      return releaseView.listRelease(releaseName);
    }

    // "/release_21/{Projects|Summary}"
    if (pathParts.size() == TWO_LEVEL_PATH_SIZE) {
      val dir = pathParts.get(2);
      switch (dir) {
      case "Projects":
        log.debug("Listing projects path '{}'", getProjectsPath(releaseName));
        return releaseView.listReleaseProjects(releaseName);
      case "Summary":
        log.debug("Listing projects path '{}'", getSummaryPath(releaseName));
        return releaseView.listReleaseSummary(releaseName);
      default:
        throwBadRequestException(format("Malformed path '%s'", path));
        return null; // Doesn't come to this point by makes PMD happy.
      }
    }

    // /release_21/Projects/TST1-CA
    if (pathParts.size() == THREE_LEVEL_PATH_SIZE) {
      val project = pathParts.get(3);

      return releaseView.listProject(releaseName, project);
    }

    val message = format("Malformed path '%s'", path);
    log.warn(message);
    throw new BadRequestException(message);
  }

  private boolean isLegacyRelease(String legacyRelease) {
    return DfsPaths.isLegacyRelease(releases, legacyRelease);
  }

}
