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
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.server.utils.Releases.getActualReleaseName;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.fs.DownloadFilesReader;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DfsPaths;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;

public class FileSystemService {

  private final AtomicReference<Map<String, Table<String, DownloadDataType, DataTypeFile>>> releaseDonorFileTypes;
  private final AtomicReference<Map<String, Multimap<String, String>>> releaseProjectDonors;
  private final AtomicReference<Map<String, Long>> releaseTimes;
  private final AtomicReference<String> currentRelease;

  public FileSystemService(@NonNull DownloadFilesReader reader) {
    this.releaseDonorFileTypes = new AtomicReference<Map<String, Table<String, DownloadDataType, DataTypeFile>>>(
        reader.getReleaseDonorFileTypes());
    this.releaseProjectDonors =
        new AtomicReference<Map<String, Multimap<String, String>>>(reader.getReleaseProjectDonors());
    this.releaseTimes = new AtomicReference<Map<String, Long>>(reader.getReleaseTimes());
    validateIntegrity();

    this.currentRelease = new AtomicReference<String>(resolveCurrentRelease());
  }

  public String getCurrentRelease() {
    return currentRelease.get();
  }

  public void loadRelease(@NonNull String releaseName, @NonNull DownloadFilesReader reader) {
    val nextReleaseDonorFileTypes = resolveReleaseDonorFileType(releaseDonorFileTypes.get(), reader, releaseName);
    val nextReleaseProjectDonors = DownloadFilesReader.getReleaseProjectDonors(nextReleaseDonorFileTypes);
    val nextReleaseTimes = resolveReleaseTimes(releaseTimes.get(), reader, releaseName);

    this.releaseDonorFileTypes.set(nextReleaseDonorFileTypes);
    this.releaseProjectDonors.set(nextReleaseProjectDonors);
    this.releaseTimes.set(nextReleaseTimes);
    validateIntegrity();
    this.currentRelease.set(resolveCurrentRelease());
  }

  public Optional<List<String>> getReleaseProjects(@NonNull String release) {
    val projectDonors = releaseProjectDonors.get().get(release);
    if (projectDonors == null) {
      return Optional.empty();
    }

    // Return sorted
    return Optional.of(copyOf(newTreeSet(projectDonors.keySet())));
  }

  public Optional<Long> getReleaseDate(@NonNull String release) {
    val releaseTime = releaseTimes.get().get(getActualReleaseName(release, getCurrentRelease()));
    if (releaseTime == null) {
      return Optional.empty();
    }

    return Optional.of(releaseTime);
  }

  public Map<DownloadDataType, Long> getClinicalSizes(@NonNull String release) {
    val donorFileTypes = releaseDonorFileTypes.get().get(release);
    checkNotNull(donorFileTypes);

    return DownloadDataType.CLINICAL.stream()
        .map(clinical -> Maps.immutableEntry(clinical, getClinicalSize(donorFileTypes.column(clinical))))
        .filter(entry -> entry.getValue() > 0)
        .collect(toImmutableMap(e -> e.getKey(), e -> e.getValue()));
  }

  public Map<DownloadDataType, Long> getProjectSizes(@NonNull String release, @NonNull String project) {
    val projectDonors = releaseProjectDonors.get().get(release);
    val donorFileTypes = releaseDonorFileTypes.get().get(release);

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

  /**
   * Unlike {@link FileSystemService#getDataTypeFiles(String, Collection, Collection)} the returned files are not
   * grouped. Thus, the method is used only to estimate download data type sizes.
   */
  public List<DataTypeFile> getUnsortedDataTypeFiles(@NonNull String release, @NonNull Collection<String> donors,
      @NonNull Collection<DownloadDataType> dataTypes) {
    val donorFileTypes = releaseDonorFileTypes.get().get(release);

    val allDataTypes = dataTypes.size() == DownloadDataType.values().length;
    val allDonors = donors.size() == donorFileTypes.rowKeySet().size();
    val donorsStream = donorFileTypes.rowMap().entrySet().stream();

    if (allDonors && allDataTypes) {
      return donorsStream
          .flatMap(entry -> entry.getValue().values().stream())
          .collect(toImmutableList());
    }

    if (allDataTypes) {
      return donorsStream
          .filter(entry -> donors.contains(entry.getKey()))
          .flatMap(entry -> entry.getValue().values().stream())
          .collect(toImmutableList());
    }

    return donorsStream
        .filter(entry -> donors.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().entrySet().stream())
        .filter(entry -> dataTypes.contains(entry.getKey()))
        .map(entry -> entry.getValue())
        .collect(toImmutableList());
  }

  /**
   * Returns a {@code DataTypeFile} list where the data type files are grouped according to {@code DownloadDataType}.
   * This makes them possible to be streamed to the client as a single data type archive.
   */
  public List<DataTypeFile> getDataTypeFiles(@NonNull String release, @NonNull Collection<String> donors,
      @NonNull Collection<DownloadDataType> dataTypes) {
    val donorFileTypes = releaseDonorFileTypes.get().get(release);

    return dataTypes.stream()
        .flatMap(dataType -> donorFileTypes.column(dataType).entrySet().stream())
        .filter(donorFileTypeEntry -> donors.contains(donorFileTypeEntry.getKey()))
        .map(donorFileTypeEntry -> donorFileTypeEntry.getValue())
        .collect(toImmutableList());
  }

  public List<DataTypeFile> getDataTypeFiles(
      @NonNull String release,
      @NonNull Set<String> project,
      @NonNull DownloadDataType dataType) {
    val projectDonors = releaseProjectDonors.get().get(release);
    val donors = resolveDonors(projectDonors, project);

    return getDataTypeFiles(release, donors, Collections.singleton(dataType));
  }

  public boolean isLegacyRelease(@NonNull String release) {
    return DfsPaths.isLegacyRelease(releaseDonorFileTypes.get().keySet(), release);
  }

  public Collection<String> getReleases() {
    return releaseDonorFileTypes.get().keySet();
  }

  public boolean existsProject(@NonNull String release, @NonNull String project) {
    val projects = releaseProjectDonors.get().get(release);

    return projects != null && projects.containsKey(project);
  }

  private void validateIntegrity() {
    val releaseTimesSize = releaseTimes.get().size();
    val releaseProjectDonorsSize = releaseProjectDonors.get().size();
    val releaseDonorFileTypesSize = releaseDonorFileTypes.get().size();
    checkState(releaseTimesSize == releaseProjectDonorsSize);
    checkState(releaseTimesSize == releaseDonorFileTypesSize);
  }

  private String resolveCurrentRelease() {
    val latestRelease = releaseTimes.get().keySet().stream()
        .max(Ordering.natural());
    checkState(latestRelease.isPresent(), "Failed to resolve current release");

    return latestRelease.get();
  }

  private static long getClinicalSize(Map<String, DataTypeFile> donorFileTypes) {
    return donorFileTypes.values().stream()
        .mapToLong(file -> file.getTotalSize())
        .sum();
  }

  private static Collection<String> resolveDonors(Multimap<String, String> projectDonors, Set<String> projects) {
    if (projectDonors.size() == projects.size()) {
      return projectDonors.values();
    }

    return projectDonors.asMap().entrySet().stream()
        .filter(entry -> projects.contains(entry.getKey()))
        .flatMap(entry -> entry.getValue().stream())
        .collect(toImmutableList());
  }

  private static Map<String, Table<String, DownloadDataType, DataTypeFile>> resolveReleaseDonorFileType(
      Map<String, Table<String, DownloadDataType, DataTypeFile>> oldReleaseDonorFileTypes,
      DownloadFilesReader reader,
      String releaseName) {
    val donorFileTypes = Maps.newHashMap(oldReleaseDonorFileTypes);
    donorFileTypes.put(releaseName, reader.createReleaseCache(releaseName));

    return ImmutableMap.copyOf(donorFileTypes);
  }

  private static Map<String, Long> resolveReleaseTimes(Map<String, Long> oldReleaseTimes,
      DownloadFilesReader reader, String releaseName) {
    val releaseTimes = Maps.newHashMap(oldReleaseTimes);
    releaseTimes.put(releaseName, reader.getReleaseTime(releaseName));

    return ImmutableMap.copyOf(releaseTimes);
  }

}
