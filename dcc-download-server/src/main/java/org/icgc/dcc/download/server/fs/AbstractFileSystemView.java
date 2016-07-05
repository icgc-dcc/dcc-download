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
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isDirectory;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsDir;
import static org.icgc.dcc.download.core.model.DownloadFileType.DIRECTORY;
import static org.icgc.dcc.download.core.model.DownloadFileType.FILE;
import static org.icgc.dcc.download.server.utils.Responses.throwPathNotFoundException;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.DownloadFileType;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.icgc.dcc.download.server.utils.DfsPaths;
import org.icgc.dcc.download.server.utils.HadoopUtils2;

import com.google.common.collect.Ordering;

@Slf4j
public abstract class AbstractFileSystemView {

  public static final String RELEASE_DIR_PREFIX = "release_";
  public static final String RELEASE_DIR_REGEX = RELEASE_DIR_PREFIX + ".*";

  protected static final String CURRENT_RELEASE_NAME = "current";
  protected static final String CURRENT_PATH = "/" + CURRENT_RELEASE_NAME;

  protected final String currentRelease;
  protected final FileSystem fileSystem;
  protected final FileSystemService fsService;
  protected final PathResolver pathResolver;

  public AbstractFileSystemView(
      @NonNull FileSystem fileSystem,
      @NonNull FileSystemService fsService,
      PathResolver pathResolver) {
    this(resolveCurrentRelease(pathResolver.getRootPath(), fileSystem), fileSystem, fsService, pathResolver);
  }

  public AbstractFileSystemView(@NonNull String currentRelease,
      @NonNull FileSystem fileSystem, @NonNull FileSystemService fsService, @NonNull PathResolver pathResolver) {
    verifyRootPath(pathResolver.getRootPath(), fileSystem);
    verifyCurrentRelease(currentRelease);
    log.info("Configuration: Current release - '{}'.", currentRelease);

    this.currentRelease = currentRelease;
    this.fileSystem = fileSystem;
    this.fsService = fsService;
    this.pathResolver = pathResolver;
  }

  protected DownloadFile convert2DownloadFile(Path file, boolean current) {
    val fileName = toResponseFileName(file, current);
    val type = resolveFileType(file);
    val size = type == DIRECTORY ? 0L : getFileSize(file);
    val date = getFileDate(file);

    val downloadFile = new DownloadFile(fileName, type, size, date);
    log.debug("Converted '{}' to {}", file, downloadFile);

    return downloadFile;
  }

  protected String toResponseFileName(Path file, boolean current) {
    log.debug("Getting response file name for file '{}'...", file);
    val uri = file.toUri();
    log.debug("File as URI: '{}'", uri);
    val filePath = uri.getPath();
    val fileName = filePath.replace(pathResolver.getRootDir(), Separators.EMPTY_STRING);
    log.debug("File name: {}", fileName);
    checkState(fileName.startsWith("/"), "File name is not an absolute path: '%s'", fileName);

    if (current) {
      val release = DfsPaths.getRelease(fileName);
      return fileName.replaceFirst(release, "current");
    }

    return fileName;
  }

  protected boolean isCurrentReleaseFile(DownloadFile rootFile) {
    val fileName = rootFile.getName();

    return fileName.startsWith("/" + currentRelease);
  }

  protected DownloadFile createDownloadDir(String path, String releaseName) {
    return createDownloadDir(path, getReleaseDate(releaseName));
  }

  protected DownloadFile createDownloadDir(String path, long releaseDate) {
    val dir = new DownloadFile();
    dir.setName(path);
    dir.setType(DIRECTORY);
    dir.setDate(releaseDate);

    return dir;
  }

  protected DownloadFile createDownloadFile(String path, long size, long releaseDate) {
    return createDownloadFile(path, FILE, size, releaseDate);
  }

  protected DownloadFile createDownloadFile(String path, DownloadFileType type, long size, long releaseDate) {
    val file = new DownloadFile();
    file.setName(path);
    file.setType(type);
    file.setDate(releaseDate);
    file.setSize(size);

    return file;
  }

  protected FileStatus getFileStatus(Path file) {
    return HadoopUtils2.getFileStatus(fileSystem, file);
  }

  protected Long getReleaseDate(String release) {
    val releaseDateOpt = fsService.getReleaseDate(release);
    if (!releaseDateOpt.isPresent()) {
      throwPathNotFoundException(format("Release '%s' doesn't exist.", release));
    }
    return releaseDateOpt.get();
  }

  private DownloadFileType resolveFileType(Path file) {
    return isDirectory(fileSystem, file) ? DIRECTORY : FILE;
  }

  private long getFileDate(Path file) {
    val status = getFileStatus(file);

    return status.getModificationTime();
  }

  private long getFileSize(Path file) {
    val status = getFileStatus(file);

    return status.getLen();
  }

  static String resolveCurrentRelease(Path rootPath, FileSystem fileSystem) {
    val dirs = lsDir(fileSystem, rootPath, compile(RELEASE_DIR_REGEX));
    val latestRelease = Ordering.natural().max(dirs);

    return latestRelease.getName();
  }

  private static void verifyRootPath(Path rootPath, FileSystem fileSystem) {
    checkArgument(checkExistence(fileSystem, rootPath), "Path '%s' doesn't exist.", rootPath);
  }

  private static void verifyCurrentRelease(String currentLink) {
    checkArgument(currentLink.matches(RELEASE_DIR_REGEX), "Current release link('%s') is invalid.",
        currentLink);
  }

}
