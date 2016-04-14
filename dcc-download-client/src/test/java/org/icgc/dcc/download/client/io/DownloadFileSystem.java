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
package org.icgc.dcc.download.client.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import lombok.Value;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class DownloadFileSystem {

  private final static String SEPARATOR = "/";
  private CurrentProjectSimLink releaseSymlink;

  private final static String CURRENT_DIRECTORY = ".";

  private final static int DEFAULT_BUFFER_SIZE = 32768;

  private int bufferSize;

  private Path rootPath;

  private FileSystem fs;

  public InputStream createInputStream(File relativePath, long offset) throws IOException {
    // TODO: consider rewrite it for inefficiency
    // TODO: https://issues.apache.org/jira/browse/HDFS-246
    Path file = getDownloadPath(relativePath);
    FSDataInputStream fis = fs.open(file, bufferSize);
    try {
      fis.seek(offset);
    } catch (IOException e) {
      // seek fails when the offset requested passes the file length,
      // this line guarantee we are positioned at the end of the file
      IOUtils.skip(fis, offset);
    }
    return fis;
  }

  public long getModificationTime(File relativePath) throws FileNotFoundException, IOException {
    FileStatus status = fs.getFileStatus(getDownloadPath(relativePath));
    return status.getModificationTime();
  }

  public AccessPermission getPermission(File relativePath) throws FileNotFoundException, IOException {
    FileStatus status = fs.getFileStatus(getDownloadPath(relativePath));
    String filename = status.getPath().getName();
    return AccessPermission.is(filename);
  }

  public long getSize(File relativePath) throws IOException {
    return fs.getFileStatus(getDownloadPath(relativePath)).getLen();
  }

  public boolean isFile(File relativePath) throws IOException {
    return fs.isFile(getDownloadPath(relativePath));
  }

  public List<File> listFiles(File relativePath) throws IOException {
    Builder<File> b = new ImmutableList.Builder<File>();
    // add release to the root
    if (relativePath.getPath().equals(SEPARATOR)) b.add(new File(releaseSymlink.getSymlink()));

    FileStatus[] statuses = fs.listStatus(getDownloadPath(relativePath));
    for (FileStatus status : statuses) {
      b.add(getRelativePathFromAbsolutePath((status.getPath().toUri().getPath())));
    }
    return b.build();
  }

  public File getRelativePathFromAbsolutePath(String absolutePath) {
    return new File(SEPARATOR, new File(rootPath.toString()).toURI().relativize(new File(absolutePath).toURI())
        .getPath());
  }

  private Path getDownloadPath(File relativePath) {
    relativePath = new File(SEPARATOR, releaseSymlink.resolveSymlink(relativePath.getPath()));
    String pathStr = relativePath.getPath();
    if (SEPARATOR.charAt(0) == pathStr.charAt(0)) pathStr = pathStr.substring(1);
    if (pathStr.equals("")) pathStr = CURRENT_DIRECTORY;
    return new Path(rootPath, pathStr);
  }

  @Value
  private static class CurrentProjectSimLink {

    private final String symlink;
    private final String actualPath;

    public String resolveSymlink(String relativePath) {
      if (relativePath.startsWith(symlink)) {
        relativePath = StringUtils.replaceOnce(relativePath, symlink, actualPath);
      }

      return relativePath;
    }

  }

  public enum AccessPermission {

    UNCHECKED(""), OPEN("open"), CONTROLLED("controlled");

    public String tag;

    AccessPermission(String tag) {
      this.tag = tag;
    }

    public static AccessPermission is(String filename) {
      if (filename.contains(CONTROLLED.tag)) return CONTROLLED;
      if (filename.contains(OPEN.tag)) return OPEN;
      return UNCHECKED;
    }
  }

}
