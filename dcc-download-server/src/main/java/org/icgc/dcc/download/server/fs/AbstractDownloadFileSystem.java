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
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isDirectory;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsDir;
import static org.icgc.dcc.download.server.model.DownloadFileType.DIRECTORY;
import static org.icgc.dcc.download.server.model.DownloadFileType.FILE;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Separators;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.model.DownloadFileType;
import org.icgc.dcc.download.server.service.DownloadFileSystemService;

import com.google.common.collect.Ordering;

@Slf4j
public abstract class AbstractDownloadFileSystem {

  public static final String HEADERS_DIR = "headers";
  public static final String DATA_DIR = "data";
  public static final String RELEASE_DIR_PREFIX = "release_";

  protected static final String RELEASE_DIR_REGEX = RELEASE_DIR_PREFIX + "\\d+";
  protected static final String CURRENT_RELEASE_NAME = "current";
  protected static final String CURRENT_PATH = "/" + CURRENT_RELEASE_NAME;

  protected final String rootDir;
  protected final String currentRelease;
  protected final FileSystem fileSystem;
  protected final Path rootPath;

  public AbstractDownloadFileSystem(@NonNull String rootDir, @NonNull FileSystem fileSystem,
      @NonNull DownloadFileSystemService fsService) {
    this(rootDir, resolveCurrentRelease(rootDir, fileSystem), fileSystem, fsService);
  }

  public AbstractDownloadFileSystem(@NonNull String rootDir, @NonNull String currentRelease,
      @NonNull FileSystem fileSystem, @NonNull DownloadFileSystemService fsService) {
    verifyRootPath(rootDir, fileSystem);
    verifyCurrentRelease(currentRelease);
    val rootPath = new Path(rootDir);

    checkArgument(rootPath.isAbsolute(), "The rootDir is not the absolute path: '%s'", rootDir);
    log.info("Configuration: Root - '{}'; Current release - '{}'.", rootDir, currentRelease);

    this.rootDir = rootDir;
    this.currentRelease = currentRelease;
    this.fileSystem = fileSystem;
    this.rootPath = rootPath;
  }

  protected Path toHdfsPath(@NonNull String dfsPath) {
    val childPath = dfsPath.startsWith("/") ? dfsPath.replaceFirst("/", EMPTY_STRING) : dfsPath;

    return new Path(rootPath, childPath);
  }

  protected DownloadFile convert2DownloadFile(Path file) {
    val fileName = toResponseFileName(file);
    val type = resolveFileType(file);
    val size = type == DIRECTORY ? 0L : getFileSize(file);
    val date = getFileDate(file);

    val downloadFile = new DownloadFile(fileName, type, size, date);
    log.debug("Converted '{}' to {}", file, downloadFile);

    return downloadFile;
  }

  protected String toResponseFileName(Path file) {
    log.debug("Getting response file name for file '{}'...", file);
    val uri = file.toUri();
    log.debug("File as URI: '{}'", uri);
    val filePath = uri.getPath();
    val fileName = filePath.replace(rootDir, Separators.EMPTY_STRING);
    log.debug("File name: {}", fileName);
    checkState(fileName.startsWith("/"), "File name is not an absolute path: '%s'", fileName);

    return fileName;
  }

  protected boolean isCurrentReleaseFile(DownloadFile rootFile) {
    val fileName = rootFile.getName();

    return fileName.startsWith("/" + currentRelease);
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

  private FileStatus getFileStatus(Path file) {
    val statusOpt = HadoopUtils.getFileStatus(fileSystem, file);
    checkState(statusOpt.isPresent(), "File doesn't exist. '%s'", file);

    return statusOpt.get();
  }

  static String resolveCurrentRelease(String rootPath, FileSystem fileSystem) {
    val dirs = lsDir(fileSystem, new Path(rootPath), compile(RELEASE_DIR_REGEX));
    val latestRelease = Ordering.natural().max(dirs);

    return latestRelease.getName();
  }

  private static void verifyRootPath(String rootPath, FileSystem fileSystem) {
    checkArgument(checkExistence(fileSystem, rootPath), "Path '%s' doesn't exist.", rootPath);
  }

  private static void verifyCurrentRelease(String currentLink) {
    checkArgument(currentLink.matches(RELEASE_DIR_REGEX), "Current release link('%s') is invalid.",
        currentLink);
  }

}
