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

import static com.google.common.base.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.model.DownloadFileType;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;

public abstract class AbstractFsTest extends AbstractTest {

  protected String rootDir;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    prepareInput();
    this.rootDir = new File(INPUT_TEST_FIXTURES_DIR).getAbsolutePath();
  }

  protected static DownloadFile newDir(String name) {
    return new DownloadFile(name, DownloadFileType.DIRECTORY, 0, 0);
  }

  protected static DownloadFile newFile(String name) {
    return new DownloadFile(name, DownloadFileType.FILE, 0, 0);
  }

  protected static void verifyDownloadFiles(@NonNull Collection<DownloadFile> actual,
      @NonNull Collection<DownloadFile> expected) {
    assertThat(actual).hasSameSizeAs(expected);
    for (val actualFile : actual) {
      val expectedFile = find(expected, actualFile);
      assertDownloadFiles(actualFile, expectedFile);
    }

  }

  private static void assertDownloadFiles(DownloadFile actualFile, DownloadFile expectedFile) {
    // Not asserting size and date, as we're sure they are calculated correctly in the AbstractDownloadFileSystem
    assertThat(actualFile.getName()).isEqualTo(expectedFile.getName());
    assertThat(actualFile.getType()).isEqualTo(expectedFile.getType());
  }

  private static DownloadFile find(Collection<DownloadFile> expected, DownloadFile actualFile) {
    val actualFileName = actualFile.getName();
    val expectedOpt = expected.stream()
        .filter(file -> file.getName().equals(actualFileName))
        .findFirst();
    checkState(expectedOpt.isPresent(), "Failed to find {} in {}", actualFile, expected);

    return expectedOpt.get();
  }

}
