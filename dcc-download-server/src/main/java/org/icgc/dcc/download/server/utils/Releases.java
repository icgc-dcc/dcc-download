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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;

@NoArgsConstructor(access = PRIVATE)
public final class Releases {

  public static String getActualReleaseName(@NonNull String releaseName, @NonNull String currentRelease) {
    val current = "current".equals(releaseName);
    val actualReleaseName = current ? currentRelease : releaseName;

    return actualReleaseName;
  }

  /**
   * Determines if the contents of the {@code path} is a legacy download release.
   */
  public static boolean isLegacyRelease(@NonNull FileSystem fileSystem, @NonNull Path path) {
    val dirs = HadoopUtils.lsDir(fileSystem, path);

    return dirs.stream()
        .map(p -> p.getName())
        .allMatch(dirName -> !dirName.equals(DATA_DIR));
  }

}
