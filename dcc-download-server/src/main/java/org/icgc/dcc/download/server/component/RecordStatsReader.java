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
package org.icgc.dcc.download.server.component;

import static com.google.common.base.Preconditions.checkState;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.server.config.Properties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class RecordStatsReader {

  @NonNull
  private final Properties properties;
  @NonNull
  private final FileSystem fileSystem;

  @SneakyThrows
  public Table<String, DownloadDataType, Long> readStatsTable() {
    val statsPath = new Path(properties.jobProperties().getInputDir(), "stats");
    log.debug("Reading records stats directory: {}", statsPath);

    // TODO: filter only gz-compressed files.
    val statsFiles = HadoopUtils.lsFile(fileSystem, statsPath);
    Table<String, DownloadDataType, Long> statsTable = HashBasedTable.create();
    for (val file : statsFiles) {
      log.info("Reading record statistics file: {}", file);
      @Cleanup
      val reader = getBufferedReader(fileSystem, file);
      try {
        readRecordStats(reader, statsTable);
      } catch (IllegalArgumentException e) {
        log.error("Failed to read {} records statistics file.", file);
        throw e;
      }
    }

    return statsTable;
  }

  @SneakyThrows
  private static void readRecordStats(BufferedReader reader, Table<String, DownloadDataType, Long> statsTable) {
    String line = null;
    while ((line = reader.readLine()) != null) {
      log.debug("Parsing line: {}", line);
      val parts = Splitters.TAB.splitToList(line);
      ensureStatsFileFormat(parts);

      val donorId = parts.get(0);
      val type = DownloadDataType.valueOf(parts.get(1));
      val count = Long.valueOf(parts.get(2));

      val currentValue = statsTable.get(donorId, type);
      checkState(currentValue == null, "Record stats for donor '%s' and type '%s' already exists(%s).", donorId, type,
          currentValue);
      statsTable.put(donorId, type, count);
    }
  }

  private static void ensureStatsFileFormat(List<String> parts) {
    val partsNum = parts.size();
    checkState(partsNum == 3, "Malformed records statistics file. Expected 3 columns, but got {}", partsNum);
  }

  @SneakyThrows
  private static BufferedReader getBufferedReader(FileSystem fileSystem, Path input) {
    val gzip = new GZIPInputStream(fileSystem.open(input));

    return new BufferedReader(new InputStreamReader(gzip));
  }

  @SneakyThrows
  public Map<DownloadDataType, Integer> resolveRecordWeights() {
    val recordWeightsFile = properties.downloadServerProperties().getRecordWeightsFile();
    val fileReader = createrRecordWeightsFileReader(recordWeightsFile);
    String line = null;
    val recordWeights = ImmutableMap.<DownloadDataType, Integer> builder();
    while ((line = fileReader.readLine()) != null) {
      recordWeights.put(parseEntry(line));
    }

    return recordWeights.build();
  }

  private static Entry<? extends DownloadDataType, ? extends Integer> parseEntry(String line) {
    val parts = Splitters.TAB.splitToList(line);
    val partsNum = parts.size();
    checkState(partsNum == 2, "Malformed records weights file. Expected 2 columns, but got %s", partsNum);
    val type = DownloadDataType.valueOf(parts.get(0));
    val weight = Integer.valueOf(parts.get(1));

    return Maps.immutableEntry(type, weight);
  }

  @SneakyThrows
  private static BufferedReader createrRecordWeightsFileReader(String recordWeightsFile) {
    val file = new File(recordWeightsFile);
    checkState(file.canRead(), "Failed to read " + file.getAbsolutePath());

    return new BufferedReader(new FileReader(file));
  }

}
