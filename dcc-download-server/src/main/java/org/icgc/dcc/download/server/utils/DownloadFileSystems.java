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
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_PREFIX;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.HEADERS_DIR;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.Path;

@NoArgsConstructor(access = PRIVATE)
public final class DownloadFileSystems {

  public static boolean isReleaseDir(List<Path> files) {
    boolean hasHeaders = false;
    boolean hasData = false;

    for (val file : files) {
      val name = file.getName();
      if (HEADERS_DIR.equals(name)) {
        hasHeaders = true;
      } else if (DATA_DIR.equals(name)) {
        hasData = true;
      }
    }

    return hasHeaders && hasData;
  }

  public static String toDfsPath(@NonNull Path fsPath) {
    return toDfsPath(fsPath.toString());
  }

  public static String toDfsPath(@NonNull String fsPath) {
    val start = fsPath.indexOf(RELEASE_DIR_PREFIX);
    checkState(start > 0);

    // Include '/'
    return fsPath.substring(start - 1);
  }

}
