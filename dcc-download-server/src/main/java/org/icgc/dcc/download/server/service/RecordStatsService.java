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

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.core.model.DownloadDataType.CLINICAL;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.download.core.model.DownloadDataType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

@RequiredArgsConstructor
public class RecordStatsService {

  /**
   * Dependencies.
   */
  @NonNull
  private final Table<String, DownloadDataType, Long> statsTable;
  @NonNull
  private final Map<DownloadDataType, Integer> recordWeights;

  public Map<DownloadDataType, Long> getRecordsSizes(@NonNull Collection<String> donorIds) {
    val sizes = donorIds.stream()
        .flatMap(donorId -> statsTable.row(donorId).entrySet().stream())
        .map(e -> convertToBytes(e))
        .collect(() -> Maps.<DownloadDataType, Long> newHashMap(),
            RecordStatsService::accumulate,
            RecordStatsService::combine);

    return combineClinical(sizes);
  }

  private Map.Entry<DownloadDataType, Long> convertToBytes(Entry<DownloadDataType, Long> entry) {
    val type = entry.getKey();
    val value = entry.getValue();
    val weigth = recordWeights.get(type);

    return Maps.immutableEntry(type, value * weigth);
  }

  private static Map<DownloadDataType, Long> combineClinical(Map<DownloadDataType, Long> sizes) {
    val combinedSizes = ImmutableMap.<DownloadDataType, Long> builder();
    combinedSizes.putAll(filterNonClinical(sizes));
    val clinicalSize = filterClinical(sizes).entrySet().stream()
        .mapToLong(e -> e.getValue().longValue())
        .sum();

    combinedSizes.put(DownloadDataType.DONOR, clinicalSize);

    return combinedSizes.build();
  }

  private static <V> Map<DownloadDataType, V> filterNonClinical(Map<DownloadDataType, V> dataTypesMap) {
    return dataTypesMap.entrySet().stream()
        .filter(e -> CLINICAL.contains(e.getKey()) == false)
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  private static <V> Map<DownloadDataType, V> filterClinical(Map<DownloadDataType, V> dataTypesMap) {
    return dataTypesMap.entrySet().stream()
        .filter(e -> CLINICAL.contains(e.getKey()))
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  private static void accumulate(Map<DownloadDataType, Long> accumulator, Map.Entry<DownloadDataType, Long> entry) {
    val type = entry.getKey();
    val value = entry.getValue();
    val currentValue = accumulator.get(type);
    accumulator.put(type, add(currentValue, value));
  }

  private static void combine(Map<DownloadDataType, Long> left, Map<DownloadDataType, Long> right) {
    right.entrySet().stream()
        .forEach(e -> accumulate(left, e));
  }

  private static Long add(Long currentValue, Long value) {
    return currentValue == null ? value : currentValue + value;
  }

}
