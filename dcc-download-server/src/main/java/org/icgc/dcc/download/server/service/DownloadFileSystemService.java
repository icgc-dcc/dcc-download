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

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Sets.newTreeSet;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.fs.DownloadFilesReader;
import org.icgc.dcc.download.server.model.DataTypeFile;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;

public class DownloadFileSystemService {

  private final Map<String, Table<String, DownloadDataType, DataTypeFile>> releaseDonorFileTypes;
  private final Map<String, Multimap<String, String>> releaseProjectDonors;
  private final Map<String, Long> releaseTimes;
  @Getter
  private final String currentRelease;

  public DownloadFileSystemService(@NonNull DownloadFilesReader reader) {
    this.releaseDonorFileTypes = reader.getReleaseDonorFileTypes();
    this.releaseProjectDonors = reader.getReleaseProjectDonors();
    this.releaseTimes = reader.getReleaseTimes();
    validateIntegrity();

    this.currentRelease = resolveCurrentRelease();
  }

  public List<String> getReleaseProjects(@NonNull String release) {
    val projectDonors = releaseProjectDonors.get(release);
    checkNotNull(projectDonors, "Failed to resolve project donors for release %s", release);

    // Return sorted
    return copyOf(newTreeSet(projectDonors.keySet()));
  }

  public long getReleaseDate(@NonNull String release) {
    val releaseTime = releaseTimes.get(release);
    checkNotNull(releaseTime, "Failed to resolve release date for release %s", release);

    return releaseTime;
  }

  public Map<DownloadDataType, Long> getClinicalSizes(@NonNull String release) {
    val donorFileTypes = releaseDonorFileTypes.get(release);
    checkNotNull(donorFileTypes);

    return DownloadDataType.CLINICAL.stream()
        .map(clinical -> Maps.immutableEntry(clinical, getClinicalSize(donorFileTypes.column(clinical))))
        .filter(entry -> entry.getValue() > 0)
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  public Map<DownloadDataType, Long> getProjectSizes(@NonNull String release, @NonNull String project) {
    val projectDonors = releaseProjectDonors.get(release);
    val donorFileTypes = releaseDonorFileTypes.get(release);

    val projectSizes = Maps.<DownloadDataType, Long> newTreeMap();
    for (val donor : projectDonors.get(project)) {
      for (val typeEntry : donorFileTypes.row(donor).entrySet()) {
        val type = typeEntry.getKey();
        val file = typeEntry.getValue();
        val size = firstNonNull(projectSizes.get(type), 0L);
        projectSizes.put(type, size + file.getTotalSize());
      }
    }

    return ImmutableMap.copyOf(projectSizes);
  }

  private void validateIntegrity() {
    val releaseTimesSize = releaseTimes.size();
    val releaseProjectDonorsSize = releaseProjectDonors.size();
    val releaseDonorFileTypesSize = releaseDonorFileTypes.size();
    checkState(releaseTimesSize == releaseProjectDonorsSize);
    checkState(releaseTimesSize == releaseDonorFileTypesSize);
  }

  private String resolveCurrentRelease() {
    val latestRelease = releaseTimes.keySet().stream()
        .max(Ordering.natural());
    checkState(latestRelease.isPresent(), "Failed to resolve current release");

    return latestRelease.get();
  }

  private static long getClinicalSize(Map<String, DataTypeFile> donorFileTypes) {
    return donorFileTypes.values().stream()
        .mapToLong(file -> file.getTotalSize())
        .sum();
  }

}
