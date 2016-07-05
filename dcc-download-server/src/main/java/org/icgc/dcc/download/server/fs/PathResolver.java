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
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.server.config.Properties.JobProperties;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DfsPaths;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Getter
@Component
public class PathResolver {

  private static final Pattern PART_FILE_PATTERN = Pattern.compile(".*part-(\\d{5})\\.gz$");

  private final Path rootPath;
  private final String rootDir;

  @Autowired
  public PathResolver(JobProperties jobProperties) {
    val rootDir = getRootDir(jobProperties.getInputDir());
    this.rootPath = new Path(rootDir.replaceFirst("/$", EMPTY_STRING));
    this.rootDir = rootPath.toString();
    checkArgument(rootPath.isAbsolute(), "The rootDir is not the absolute path: '%s'", rootDir);
    log.info("Configuration: rootPath - {}", rootPath);
  }

  public List<String> getPartFilePaths(@NonNull String release, @NonNull DataTypeFile dataFile) {
    val indices = dataFile.getPartFileIndices();

    return indices.stream()
        .map(index -> indexToPartFile(index))
        .map(partFile -> Joiners.PATH.join(rootDir, release, DATA_DIR, dataFile.getPath(), partFile))
        .collect(toImmutableList());

  }

  public Path toHdfsPath(@NonNull String dfsPath) {
    DfsPaths.validatePath(dfsPath);
    val childPath = dfsPath.startsWith("/") ? dfsPath.replaceFirst("/", EMPTY_STRING) : dfsPath;

    return new Path(rootPath, childPath);
  }

  /**
   * Lists files on path {@code dfsPath}. Does not validate if the {@code dfsPath} complies to the release directory
   * structure.
   */
  public Path toLegacyHdfsPath(@NonNull String dfsPath) {
    val childPath = dfsPath.startsWith("/") ? dfsPath.replaceFirst("/", EMPTY_STRING) : dfsPath;

    return new Path(rootPath, childPath);
  }

  public String toDfsPath(@NonNull Path hdfsPath) {
    return removeRootDirPrefix(hdfsPath.toString());
  }

  public String getDataFilePath(@NonNull String file) {
    val dfsPath = removeRootDirPrefix(file);
    log.debug("'{}' without prefix: '{}'", file, dfsPath);
    val pathParts = Splitters.PATH.splitToList(dfsPath);
    log.debug("Parts: {}", pathParts);

    return Joiners.PATH.join(pathParts.get(3), pathParts.get(4), pathParts.get(5));
  }

  public Short getPartFileIndex(String partFile) {
    val matcher = PART_FILE_PATTERN.matcher(partFile);
    checkState(matcher.matches());
    val index = matcher.group(1);

    return Short.valueOf(index);
  }

  private String removeRootDirPrefix(String file) {
    val startOfRoot = file.indexOf(rootDir);
    val schema = file.substring(0, startOfRoot);
    return file
        .replaceFirst(schema + rootDir, EMPTY_STRING);
  }

  private static String getRootDir(String inputDir) {
    return inputDir.startsWith("/") ? inputDir : new File(inputDir).getAbsolutePath();
  }

  private static String indexToPartFile(Short index) {
    return format("part-%05d.gz", index);
  }

}
