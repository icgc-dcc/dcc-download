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
import static org.icgc.dcc.download.server.fs.AbstractFileSystemView.RELEASE_DIR_REGEX;

import java.util.regex.Pattern;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.core.util.Splitters;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@NoArgsConstructor(access = PRIVATE)
public final class DfsPaths {

  private static final Pattern RELEASE_PATTERN = Pattern.compile(RELEASE_DIR_REGEX + "|current");
  private static final Pattern RELEASE_DIR_PATTERN = Pattern.compile("Projects|Summary");
  private static final Pattern PROJECT_NAME_PATTERN = Pattern.compile("\\w{2,4}-\\w{2}");

  private static final BiMap<DownloadDataType, String> FILE_NAMES = defineFileNames();

  public static String getFileName(@NonNull DownloadDataType dataType) {
    val fileName = FILE_NAMES.get(dataType);
    checkNotNull(fileName, "Failed to resolve file name from download data type %s", dataType);

    return fileName;
  }

  public static String getRelease(String path) {
    val pathParts = Splitters.PATH.splitToList(path);

    return pathParts.get(1);

  }

  public static String getProject(String path) {
    val pathParts = Splitters.PATH.splitToList(path);

    return pathParts.get(3);
  }

  public static DownloadDataType getDownloadDataType(String path) {
    val pathParts = Splitters.PATH.splitToList(path);
    val fileName = pathParts.get(4);

    return resolveDownloadDataType(fileName);
  }

  private static DownloadDataType resolveDownloadDataType(String fileName) {
    val archiveName = fileName
        .replaceFirst(".tsv.gz", EMPTY_STRING)
        .replaceFirst(PROJECT_NAME_PATTERN.pattern(), EMPTY_STRING)
        .replaceFirst(".$", EMPTY_STRING);

    val downloadDataTypes = FILE_NAMES.entrySet().stream()
        .filter(entry -> entry.getValue().equals(archiveName))
        .map(entry -> entry.getKey())
        .collect(toImmutableList());
    checkState(downloadDataTypes.size() == 1, "Failed to resolve DownloadDataType form file name '%s'", fileName);

    return downloadDataTypes.get(0);
  }

  public static void validatePath(String path) {
    if ("/".equals(path)) {
      return;
    }

    val pathParts = Splitters.PATH.splitToList(path);
    checkArgument(pathParts.size() < 6, "Invalid path '%s'", path);
    for (int i = 0; i < pathParts.size(); i++) {
      verifyPathPart(i, pathParts.get(i));
    }
  }

  private static void verifyPathPart(int i, String part) {
    switch (i) {
    case 0:
      checkArgument(part.isEmpty());
      break;
    case 1:
      checkArgument(RELEASE_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          RELEASE_PATTERN);
      break;
    case 2:
      checkArgument(RELEASE_DIR_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          RELEASE_DIR_PATTERN);
      break;
    case 3:
      // TODO: allow donor.all_projects.tsv.gz in path "/release_21/Summary/donor.all_projects.tsv.gz"
      checkArgument(PROJECT_NAME_PATTERN.matcher(part).matches(), "'%s' doesn't match release pattern %s", part,
          PROJECT_NAME_PATTERN);
      break;
    case 4:
      // TODO: verify download file name
      break;
    default:
      throw new IllegalArgumentException(format("Unexpected argument: %s at position %s", part, i));
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
