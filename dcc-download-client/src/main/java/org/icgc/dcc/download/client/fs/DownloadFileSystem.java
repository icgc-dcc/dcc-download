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
package org.icgc.dcc.download.client.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

@RequiredArgsConstructor
public class DownloadFileSystem {

  /**
   * Constants.
   */
  public static final String DEFAULT_ROOT_DIR = "/icgc/download/static";
  private static final String SEPARATOR = "/";
  private static final String CURRENT_DIRECTORY = ".";
  private static final int DEFAULT_BUFFER_SIZE = 32768;

  /**
   * Dependencies.
   */
  private final CurrentProjectSimLink releaseSymlink;
  private final FileSystem fs;
  private final Path rootPath;
  private final int bufferSize;

  public DownloadFileSystem(@NonNull CurrentProjectSimLink releaseSymlink, @NonNull FileSystem fs,
      @NonNull Path rootPath) {
    this(releaseSymlink, fs, rootPath, DEFAULT_BUFFER_SIZE);
  }

  public InputStream createInputStream(@NonNull File relativePath, long offset) throws IOException {
    // TODO: consider rewrite it for inefficiency
    // TODO: https://issues.apache.org/jira/browse/HDFS-246
    val file = getDownloadPath(relativePath);
    val fis = fs.open(file, bufferSize);

    try {
      fis.seek(offset);
    } catch (IOException e) {
      // Seek fails when the offset requested passes the file length,
      // this line guarantee we are positioned at the end of the file
      ByteStreams.skipFully(fis, offset);
    }

    return fis;
  }

  public long getModificationTime(@NonNull File relativePath) throws FileNotFoundException, IOException {
    val status = fs.getFileStatus(getDownloadPath(relativePath));

    return status.getModificationTime();
  }

  public AccessPermission getAccessPermission(@NonNull File relativePath) throws FileNotFoundException, IOException {
    val status = fs.getFileStatus(getDownloadPath(relativePath));
    val filename = status.getPath().getName();

    return AccessPermission.from(filename);
  }

  public long getSize(@NonNull File relativePath) throws IOException {
    return fs.getFileStatus(getDownloadPath(relativePath))
        .getLen();
  }

  public boolean isFile(@NonNull File relativePath) throws IOException {
    val fileStatus = fs.getFileStatus(getDownloadPath(relativePath));

    return fileStatus.isFile();
  }

  public List<File> listFiles(@NonNull File relativePath) throws IOException {
    val files = ImmutableList.<File> builder();
    // Add release to the root
    if (relativePath.getPath().equals(SEPARATOR)) {
      files.add(new File(releaseSymlink.getSymlink()));
    }

    FileStatus[] statuses = fs.listStatus(getDownloadPath(relativePath));
    for (val status : statuses) {
      files.add(getRelativePathFromAbsolutePath((status.getPath().toUri().getPath())));
    }

    return files.build();
  }

  public boolean exists(@NonNull File relativePath) throws IOException {
    return fs.exists(getDownloadPath(relativePath));
  }

  private File getRelativePathFromAbsolutePath(@NonNull String absolutePath) {
    return new File(SEPARATOR, new File(rootPath.toString())
        .toURI()
        .relativize(new File(absolutePath).toURI())
        .getPath());
  }

  private Path getDownloadPath(File relativePath) {
    relativePath = new File(SEPARATOR, releaseSymlink.resolveSymlink(relativePath.getPath()));
    String pathStr = relativePath.getPath();
    if (pathStr.startsWith(SEPARATOR)) {
      pathStr = pathStr.substring(1);
    }

    if (pathStr.isEmpty()) {
      pathStr = CURRENT_DIRECTORY;
    }

    return new Path(rootPath, pathStr);
  }

}
