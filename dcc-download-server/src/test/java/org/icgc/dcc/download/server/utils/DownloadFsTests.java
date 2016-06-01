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

import static com.google.common.collect.ImmutableList.of;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

@NoArgsConstructor(access = PRIVATE)
public final class DownloadFsTests {

  public static Table<String, DownloadDataType, DataTypeFile> createDonorFileTypesTable() {
    val releaseTable = HashBasedTable.<String, DownloadDataType, DataTypeFile> create();
    releaseTable.put("DO001", DONOR,
        new DataTypeFile("/somepath/release_21/TST1-CA/DO001/donor", of("part-00000.gz"), 1));
    releaseTable.put("DO001", SAMPLE,
        new DataTypeFile("/somepath/release_21/TST1-CA/DO001/sample", of("part-00000.gz"), 2));
    releaseTable.put("DO002", DONOR,
        new DataTypeFile("/somepath/release_21/TST1-CA/DO002/donor", of("part-00000.gz", "part-00001.gz"), 3));
    releaseTable.put("DO003", DONOR,
        new DataTypeFile("/somepath/release_21/TST2-CA/DO003/donor", of("part-00000.gz"), 4));

    return releaseTable;
  }

  public static Multimap<String, String> createProjectDonors() {
    val projectDonors = ArrayListMultimap.<String, String> create();
    projectDonors.put("TST1-CA", "DO001");
    projectDonors.put("TST1-CA", "DO002");
    projectDonors.put("TST2-CA", "DO003");

    return projectDonors;
  }

}
