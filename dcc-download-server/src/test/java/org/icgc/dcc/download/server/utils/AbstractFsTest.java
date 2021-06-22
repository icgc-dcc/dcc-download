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
package org.icgc.dcc.download.server.utils;

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.server.utils.DateUtils.isEpochsEqualUpToSecondsDigits;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.DownloadFileType;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

public abstract class AbstractFsTest extends AbstractTest {

  protected String rootDir;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    prepareInput();
    this.rootDir = workingDir.getAbsolutePath();
  }

  protected long getModificationTime(@NonNull String path) {
    return getModificationTime(new File(workingDir, path));
  }

  public static DownloadFile newDir(String name) {
    return new DownloadFile(name, DownloadFileType.DIRECTORY, 0, 0, null);
  }

  public static DownloadFile newDir(String name, long creationDate) {
    return newDir(name, 0, creationDate);
  }

  public static DownloadFile newDir(String name, long size, long creationDate) {
    return new DownloadFile(name, DownloadFileType.DIRECTORY, size, creationDate, null);
  }

  public static DownloadFile newFile(String name, long size, long creationDate) {
    return new DownloadFile(name, DownloadFileType.FILE, size, creationDate, null);
  }

  protected static void verifyDownloadFiles(@NonNull List<DownloadFile> actual,
      @NonNull List<DownloadFile> expected) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      val actualFile = actual.get(i);
      val expectedFile = expected.get(i);
      assertDownloadFiles(actualFile, expectedFile);
    }
  }

  private static void assertDownloadFiles(DownloadFile actualFile, DownloadFile expectedFile) {
    assertThat(actualFile.getName()).isEqualTo(expectedFile.getName());
    assertThat(actualFile.getType()).isEqualTo(expectedFile.getType());
    assertThat(isEpochsEqualUpToSecondsDigits(actualFile.getDate(), expectedFile.getDate())).isTrue();
    assertThat(actualFile.getSize()).isEqualTo(expectedFile.getSize());
  }

  @SneakyThrows
  public static long getModificationTime(@NonNull File file) {
    checkState(file.exists(), "File doesn't exist: %s", file.getAbsolutePath());
    val fileTime = Files.getLastModifiedTime(Paths.get(file.toURI()));

    return fileTime.toMillis();
  }

}
