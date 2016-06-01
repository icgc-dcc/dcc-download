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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;

import java.util.Collection;
import java.util.regex.Pattern;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.service.DownloadFileSystemService;
import org.icgc.dcc.download.server.utils.DownloadFileSystems;

@Slf4j
public class DownloadFileSystem extends AbstractDownloadFileSystem {

  private static final Pattern RELEASE_PATTERN = Pattern.compile(RELEASE_DIR_REGEX + "|current");
  private static final Pattern RELEASE_DIR_PATTERN = Pattern.compile("Projects|Summary");
  private static final Pattern PROJECT_NAME_PATTERN = Pattern.compile("\\w{2,4}-\\w{2}");

  private final RootView rootView;
  private final ViewController viewController;

  public DownloadFileSystem(@NonNull String rootDir, @NonNull FileSystem fileSystem,
      @NonNull DownloadFileSystemService fsService, @NonNull RootView rootView, ViewController viewController) {
    super(rootDir, fileSystem, fsService);
    this.rootView = rootView;
    this.viewController = viewController;
  }

  public Collection<DownloadFile> listFiles(@NonNull String path) {
    log.debug("Listing files for path '{}'...", path);
    verifyPath(path);

    val fsPath = toFsPath(path);
    log.debug("Listing files for real path '{}'...", fsPath);

    val files = HadoopUtils.lsAll(fileSystem, fsPath);
    log.debug("{}: {}", path, files);

    if (DownloadFileSystems.isReleaseDir(files)) {
      // project
    } else {
      return rootView.listReleases();
    }

    return viewController.listFiles(path);
  }

  private Path toFsPath(String path) {
    val relativePath = relativize(path);

    return EMPTY_STRING.equals(relativePath) ? rootPath : new Path(rootPath, relativePath);
  }

  private static String relativize(String path) {
    // TODO: user toDfs
    return path.replaceFirst("^/", EMPTY_STRING);
  }

  static void verifyPath(String path) {
    if ("/".equals(path)) {
      return;
    }

    val pathParts = Splitters.PATH.splitToList(path);
    checkArgument(pathParts.size() < 5, "Invalid path '%s'", path);
    for (int i = 0; i < pathParts.size(); i++) {
      verifyPathPart(i, pathParts.get(i));
    }
  }

  private static void verifyPathPart(int i, String part) {
    switch (i) {
    case 0:
      checkArgument(part.isEmpty());
      break;
    case 1:
      checkArgument(RELEASE_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          RELEASE_PATTERN);
      break;
    case 2:
      checkArgument(RELEASE_DIR_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          RELEASE_DIR_PATTERN);
      break;
    case 3:
      checkArgument(PROJECT_NAME_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          PROJECT_NAME_PATTERN);
      break;
    default:
      throw new IllegalArgumentException(format("Unexpected argument: %s at position %s", part, i));
    }

  }

}
