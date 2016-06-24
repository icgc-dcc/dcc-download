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
import static org.icgc.dcc.common.hadoop.fs.FileSystems.getDefaultLocalFileSystem;
import lombok.val;

import org.icgc.dcc.download.server.config.Properties;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.icgc.dcc.download.server.utils.AbstractFsTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RootViewTest extends AbstractFsTest {

  @Mock
  FileSystemService fsService;
  RootView rootView;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    val properties = new Properties.JobProperties();
    properties.setInputDir(workingDir.getAbsolutePath());
    val pathResolver = new PathResolver(properties);
    this.rootView = new RootView(getDefaultLocalFileSystem(), fsService, pathResolver);
  }

  @Test
  public void testListReleases() throws Exception {
    val releases = rootView.listReleases();
    verifyDownloadFiles(releases, of(
        newFile("/README.txt", 18L, getModificationTime("README.txt")),
        newDir("/current", getModificationTime("release_21")),
        newDir("/release_20", getModificationTime("release_20")),
        newDir("/release_21", getModificationTime("release_21"))));
  }

}
