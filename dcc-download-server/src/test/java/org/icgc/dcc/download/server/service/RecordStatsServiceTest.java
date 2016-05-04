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
package org.icgc.dcc.download.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;

import java.util.Map;

import lombok.val;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;

public class RecordStatsServiceTest {

  RecordStatsService service;

  @Before
  public void setUp() {
    service = new RecordStatsService(defineStatsTable(), defineRecordWeights());
  }

  @Test
  public void testGetRecordsSizes() throws Exception {
    val donorIds = ImmutableSet.of("DO1", "DO2");
    val sizes = service.getRecordsSizes(donorIds);
    assertThat(sizes).hasSize(1);
    // DO1: (Donor) 1 * 1 + (Specimen) 2 * 2 + (Sample) 2 * 3 = 12
    // DO2: (Donor) 1 * 1 = 1
    assertThat(sizes.get(DONOR)).isEqualTo(13);

  }

  private static Table<String, DownloadDataType, Long> defineStatsTable() {
    Table<String, DownloadDataType, Long> statsTable = HashBasedTable.create();
    statsTable.put("DO1", DownloadDataType.DONOR, 1L);
    statsTable.put("DO2", DownloadDataType.DONOR, 2L);
    statsTable.put("DO1", DownloadDataType.SPECIMEN, 2L);
    statsTable.put("DO1", DownloadDataType.SAMPLE, 2L);

    return statsTable;
  }

  private Map<DownloadDataType, Integer> defineRecordWeights() {
    return ImmutableMap.of(DONOR, 1, DownloadDataType.SPECIMEN, 2, DownloadDataType.SAMPLE, 3);
  }

}
