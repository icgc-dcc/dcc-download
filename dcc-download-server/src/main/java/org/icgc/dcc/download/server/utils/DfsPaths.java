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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.model.DownloadDataType.CNSM;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR_EXPOSURE;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR_FAMILY;
import static org.icgc.dcc.common.core.model.DownloadDataType.DONOR_THERAPY;
import static org.icgc.dcc.common.core.model.DownloadDataType.EXP_ARRAY;
import static org.icgc.dcc.common.core.model.DownloadDataType.EXP_SEQ;
import static org.icgc.dcc.common.core.model.DownloadDataType.JCN;
import static org.icgc.dcc.common.core.model.DownloadDataType.METH_ARRAY;
import static org.icgc.dcc.common.core.model.DownloadDataType.METH_SEQ;
import static org.icgc.dcc.common.core.model.DownloadDataType.MIRNA_SEQ;
import static org.icgc.dcc.common.core.model.DownloadDataType.PEXP;
import static org.icgc.dcc.common.core.model.DownloadDataType.SAMPLE;
import static org.icgc.dcc.common.core.model.DownloadDataType.SGV_CONTROLLED;
import static org.icgc.dcc.common.core.model.DownloadDataType.SPECIMEN;
import static org.icgc.dcc.common.core.model.DownloadDataType.SSM_CONTROLLED;
import static org.icgc.dcc.common.core.model.DownloadDataType.SSM_OPEN;
import static org.icgc.dcc.common.core.model.DownloadDataType.STSM;
import static org.icgc.dcc.common.core.util.Separators.EMPTY_STRING;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_PREFIX;
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_REGEX;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.DATA_DIR;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.HEADERS_DIR;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;
import org.icgc.dcc.download.server.endpoint.NotFoundException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class DfsPaths {

  private static final Pattern RELEASE_PATTERN = Pattern.compile(RELEASE_DIR_REGEX + "|current|README.txt");
  private static final Pattern RELEASE_DIR_PATTERN = Pattern.compile("Projects|Summary|README.txt");
  private static final Pattern PROJECT_NAME_PATTERN = Pattern.compile("\\w{2,4}-\\w{2}");
  private static final Pattern FILE_NAME_PATTERN = Pattern.compile("(README.txt|.*\\.(vcf|tsv)\\.gz)$");
  private static final Pattern REAL_FILE_NAME_PATTERN = Pattern.compile("(README.txt|.*\\.vcf\\.gz)$");

  private static final BiMap<DownloadDataType, String> FILE_NAMES = defineFileNames();

  public static String getFileName(@NonNull DownloadDataType dataType, @NonNull Optional<String> suffix) {
    val fileName = FILE_NAMES.get(dataType);
    checkNotNull(fileName, "Failed to resolve file name from download data type %s", dataType);

    return suffix.isPresent() ? fileName + suffix.get() : fileName;
  }

  public static String getRelease(String path) {
    log.debug("Resolving release from path '{}'", path);
    val pathParts = Splitters.PATH.splitToList(path);

    return getRelease(pathParts);
  }

  public static String getRelease(@NonNull List<String> pathParts) {
    checkArgument(pathParts.size() > 1, "Malformed path parts: %s", pathParts);
    val release = pathParts.get(1);
    verifyPathPart(RELEASE_PATTERN, release);

    return release;
  }

  public static String getLegacyRelease(@NonNull String path) {
    log.debug("Resolving legacy release from path '{}'", path);
    val pathParts = Splitters.PATH.splitToList(path);

    return getLegacyRelease(pathParts);
  }

  public static String getLegacyRelease(@NonNull List<String> pathParts) {
    checkArgument(pathParts.size() > 1, "Malformed path parts: %s", pathParts);

    return pathParts.get(1);
  }

  public static Optional<String> getProject(String path) {
    val pathParts = Splitters.PATH.splitToList(path);
    if (pathParts.size() == 4) {
      log.info("'{}' is a summary file. It has no project.", path);
      return Optional.empty();
    }

    val project = pathParts.get(3);
    log.debug("Resolved project '{}' from path '{}'", project, path);

    return Optional.of(project);
  }

  public static DownloadDataType getDownloadDataType(@NonNull String path) {
    log.debug("Resolving download data type from path '{}'", path);
    val pathParts = Splitters.PATH.splitToList(path);

    String fileName = null;
    if (pathParts.size() == 4) {
      fileName = pathParts.get(3);
    } else if (pathParts.size() == 5) {
      fileName = pathParts.get(4);
    } else {
      throw new IllegalArgumentException(format("Failed to resolve download data type form path '%s'. Parts: %s", path,
          pathParts));
    }

    return resolveDownloadDataType(fileName);
  }

  /**
   * Checks if the {@code path} represents a real entity on the file system. E.g. a README file or aggregated SSMs.
   */
  public static boolean isRealEntity(@NonNull String path) {
    val pathParts = Splitters.PATH.splitToList(path);
    val fileName = pathParts.get(pathParts.size() - 1);

    return REAL_FILE_NAME_PATTERN.matcher(fileName).matches();
  }

  public static boolean isReleaseDir(@NonNull List<Path> files) {
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

  public static void validatePath(String path) {
    if ("/".equals(path)) {
      return;
    }

    val pathParts = Splitters.PATH.splitToList(path);
    checkArgument(pathParts.size() < 6, "Invalid path '%s'", path);
    for (int i = 0; i < pathParts.size(); i++) {
      verifyPathPart(i, pathParts);
    }
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

  /**
   * Check if the {@code release} is a legacy release according to the {@code releases} collection.
   */
  public static boolean isLegacyRelease(@NonNull Collection<String> releases, @NonNull String release) {
    return !releases.contains(release) && !"current".equals(release);
  }

  /**
   * Creates a DFS summary path. E.g. {@code /release_21/Summary}
   */
  public static String getSummaryPath(@NonNull String release) {
    return format("/%s/Summary", release);
  }

  /**
   * Creates a DFS projects path. E.g. {@code /release_21/Projects}
   */
  public static String getProjectsPath(@NonNull String release) {
    return format("/%s/Projects", release);
  }

  /**
   * Creates a DFS project path. E.g. {@code /release_21/Projects/TST-CA}
   */
  public static String getProjectPath(@NonNull String release, @NonNull String project) {
    return format("/%s/Projects/%s", release, project);
  }

  private static DownloadDataType resolveDownloadDataType(String fileName) {
    log.debug("Resolving download data type from file name '{}'", fileName);
    val archiveName = fileName
        .replaceFirst("\\.tsv\\.gz", EMPTY_STRING)
        .replaceFirst(PROJECT_NAME_PATTERN.pattern(), EMPTY_STRING)
        .replaceFirst("all_projects", EMPTY_STRING)
        .replaceFirst("\\.$", EMPTY_STRING);
    log.debug("archiveName: '{}'", archiveName);

    val downloadDataTypes = FILE_NAMES.entrySet().stream()
        .filter(entry -> entry.getValue().equals(archiveName))
        .map(entry -> entry.getKey())
        .collect(toImmutableList());
    checkState(downloadDataTypes.size() == 1, "Failed to resolve DownloadDataType form file name '%s'", fileName);

    return downloadDataTypes.get(0);
  }

  private static void verifyPathPart(int i, List<String> parts) {
    val part = parts.get(i);
    switch (i) {
    // when path is split by '/' the first part is always empty
    case 0:
      checkArgument(part.isEmpty());
      break;
    // /release_21
    case 1:
      verifyPathPart(RELEASE_PATTERN, part);
      break;
    // /release_21/{Projects|Summary}
    case 2:
      verifyPathPart(RELEASE_DIR_PATTERN, part);
      break;
    // contents of /release_21/{Projects|Summary}
    case 3:
      val parent = parts.get(2);
      if ("Summary".equals(parent)) {
        verifyFileName(part);
      } else if ("Projects".equals(parent) && part.equals("README.txt")) {
        // Do nothing this is /<release>/Projects/README.txt
      } else {
        verifyPathPart(PROJECT_NAME_PATTERN, part);
      }
      break;
    // /release_21/Projects/TST-CA/file
    case 4:
      verifyFileName(part);
      break;
    default:
      throw new IllegalArgumentException(format("Unexpected argument: %s at position %s", part, i));
    }

  }

  private static void verifyFileName(String part) {
    verifyPathPart(FILE_NAME_PATTERN, part);
  }

  private static void verifyPathPart(Pattern pattern, String part) {
    if (!pattern.matcher(part).matches()) {
      val message = format("'%s' doesn't match release pattern %s", part, pattern);
      log.warn(message);
      throw new NotFoundException(message);
    }
  }

  private static BiMap<DownloadDataType, String> defineFileNames() {
    val fileNames = HashBiMap.<DownloadDataType, String> create();
    fileNames.put(DONOR, DONOR.getId());
    fileNames.put(SPECIMEN, SPECIMEN.getId());
    fileNames.put(SAMPLE, SAMPLE.getId());
    fileNames.put(DONOR_EXPOSURE, DONOR_EXPOSURE.getId());
    fileNames.put(DONOR_FAMILY, DONOR_FAMILY.getId());
    fileNames.put(DONOR_THERAPY, DONOR_THERAPY.getId());
    fileNames.put(CNSM, "copy_number_somatic_mutation");
    fileNames.put(EXP_ARRAY, EXP_ARRAY.getId());
    fileNames.put(EXP_SEQ, EXP_SEQ.getId());
    fileNames.put(JCN, "splice_variant");
    fileNames.put(METH_ARRAY, METH_ARRAY.getId());
    fileNames.put(METH_SEQ, METH_SEQ.getId());
    fileNames.put(MIRNA_SEQ, MIRNA_SEQ.getId());
    fileNames.put(PEXP, "protein_expression");
    fileNames.put(SGV_CONTROLLED, "simple_germline_variation.controlled");
    fileNames.put(SSM_CONTROLLED, "simple_somatic_mutation.controlled");
    fileNames.put(SSM_OPEN, "simple_somatic_mutation.open");
    fileNames.put(STSM, "structural_somatic_mutation");

    checkState(fileNames.size() == DownloadDataType.values().length);

    return fileNames;
  }

}
