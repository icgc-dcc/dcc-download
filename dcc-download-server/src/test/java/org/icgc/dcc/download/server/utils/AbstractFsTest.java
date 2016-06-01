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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.model.DownloadFileType;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;

import com.google.common.collect.Iterables;

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

  protected static DownloadFile newDir(String name, long creationDate) {
    return newDir(name, 0, creationDate);
  }

  protected static DownloadFile newDir(String name, long size, long creationDate) {
    return new DownloadFile(name, DownloadFileType.DIRECTORY, size, creationDate);
  }

  protected static DownloadFile newFile(String name) {
    return new DownloadFile(name, DownloadFileType.FILE, 0, 0);
  }

  protected static DownloadFile newFile(String name, long size, long creationDate) {
    return new DownloadFile(name, DownloadFileType.FILE, size, creationDate);
  }

  protected static void verifyDownloadFiles(@NonNull Collection<DownloadFile> actual,
      @NonNull Collection<DownloadFile> expected) {
    assertThat(actual).hasSameSizeAs(expected);
    for (int i = 0; i < expected.size(); i++) {
      val actualFile = Iterables.get(actual, i);
      val expectedFile = Iterables.get(expected, i);
      assertDownloadFiles(actualFile, expectedFile);
    }
  }

  private static void assertDownloadFiles(DownloadFile actualFile, DownloadFile expectedFile) {
    assertThat(actualFile.getName()).isEqualTo(expectedFile.getName());
    assertThat(actualFile.getType()).isEqualTo(expectedFile.getType());
    assertThat(actualFile.getDate()).isEqualTo(expectedFile.getDate());
    assertThat(actualFile.getSize()).isEqualTo(expectedFile.getSize());
  }

}
