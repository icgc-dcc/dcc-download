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

import static java.nio.file.Paths.get;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;

import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.common.test.file.FileTests;
import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.model.DownloadFileType;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Test;

@Slf4j
public class AbstractDownloadFileSystemTest extends AbstractTest {

  AbstractDownloadFileSystem dfs;
  FileSystem fileSystem;

  @Override
  public void setUp() {
    super.setUp();
    prepareFiles();
    this.fileSystem = getDefaultLocalFileSystem();
    this.dfs = new TestDownloadFileSystem(workingDir.getAbsolutePath(), fileSystem);
  }

  @Test
  public void testResolveCurrentRelease() throws Exception {
    val currentRelease = AbstractDownloadFileSystem.resolveCurrentRelease(workingDir.getAbsolutePath(), fileSystem);
    assertThat(currentRelease).isEqualTo("release_21");
  }

  @Test
  public void testToResponseFileName() throws Exception {
    val release20 = HadoopUtils.lsDir(fileSystem, new Path(workingDir.getAbsolutePath())).get(0);
    val responseFileName = dfs.toResponseFileName(release20);
    assertThat(responseFileName).isEqualTo("/release_20");
  }

  @Test
  public void testConvert2DownloadFile_dir() throws Exception {
    val srcFile = new File(new File(workingDir, "release_21"), "Projects");
    srcFile.mkdir();

    val dfsFile = dfs.convert2DownloadFile(new Path(srcFile.getAbsolutePath()));
    assertThat(dfsFile.getName()).isEqualTo("/release_21/Projects");
    assertThat(dfsFile.getSize()).isEqualTo(0);
    assertThat(dfsFile.getType()).isEqualTo(DownloadFileType.DIRECTORY);
    assertCreationDate(srcFile, dfsFile.getDate());
  }

  @Test
  public void testConvert2DownloadFile_file() throws Exception {
    val srcFile = new File(workingDir, "zzz123.txt");
    srcFile.createNewFile();
    val out = FileTests.getBufferedWriter(srcFile);
    out.write("Test string");
    out.close();

    val dfsFile = dfs.convert2DownloadFile(new Path(srcFile.getAbsolutePath()));
    assertThat(dfsFile.getName()).isEqualTo("/zzz123.txt");
    assertThat(dfsFile.getType()).isEqualTo(DownloadFileType.FILE);
    assertCreationDate(srcFile, dfsFile.getDate());
    assertFileSize(srcFile, dfsFile.getSize());
  }

  @Test
  public void testIsCurrentReleaseFile() throws Exception {
    val dfsFile = new DownloadFile();
    dfsFile.setName("/release_21/Projects/test.gz");
    assertThat(dfs.isCurrentReleaseFile(dfsFile)).isTrue();
  }

  private static void assertFileSize(File srcFile, long actualSize) {
    val fileAttributes = getFileAttributes(srcFile);
    val expectedSize = fileAttributes.size();
    assertThat(actualSize).isEqualTo(expectedSize);
  }

  private static void assertCreationDate(File srcFile, long actualDate) {
    val fileAttributes = getFileAttributes(srcFile);
    val creationTime = fileAttributes.creationTime();
    log.info("{}", creationTime);
    assertThat(actualDate).isEqualTo(creationTime.toMillis());
  }

  @SneakyThrows
  private static BasicFileAttributes getFileAttributes(File srcFile) {
    val filePath = get(srcFile.toURI());
    val fileAttributes = Files.readAttributes(filePath, BasicFileAttributes.class);

    return fileAttributes;
  }

  private void prepareFiles() {
    new File(workingDir, "release_21").mkdir();
    new File(workingDir, "release_20").mkdir();
  }

  private static class TestDownloadFileSystem extends AbstractDownloadFileSystem {

    public TestDownloadFileSystem(String rootDir, FileSystem fileSystem) {
      super(rootDir, fileSystem, mock(FileSystemService.class));
    }

  }

}
