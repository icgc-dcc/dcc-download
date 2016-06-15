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

import static com.google.common.collect.ImmutableList.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;

import java.io.File;
import java.util.Collection;

import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.DownloadFileType;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.icgc.dcc.download.test.AbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DownloadFileSystemTest extends AbstractTest {

  @Mock
  FileSystemService fsService;

  DownloadFileSystem dfs;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    prepareInput();
    val rootDir = new File(INPUT_TEST_FIXTURES_DIR).getAbsolutePath();
    FileSystem fs = getDefaultLocalFileSystem();
    val rootView = new RootView(rootDir, fs, fsService);
    val releaseView = new ReleaseView(rootDir, fs, fsService);
    this.dfs = new DownloadFileSystem(rootView, releaseView);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testListFiles_incorrectArguments() throws Exception {
    dfs.listFiles("zzz");
  }

  @Test
  public void testListFiles_root() throws Exception {
    verifyDownloadFiles(dfs.listFiles("/"), of(newDir("/current"), newDir("/release_21")));
  }

  private static void verifyDownloadFiles(@NonNull Collection<DownloadFile> actual,
      @NonNull Collection<DownloadFile> expected) {
    assertThat(actual).hasSameSizeAs(expected);
  }

  private DownloadFile newDir(String name) {
    return new DownloadFile(name, DownloadFileType.DIRECTORY, 0, 0);
  }

  private DownloadFile newFile(String name) {
    return new DownloadFile(name, DownloadFileType.FILE, 0, 0);
  }

}
