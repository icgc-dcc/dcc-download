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
package org.icgc.dcc.download.server.fs;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.server.model.DownloadFile;
import org.icgc.dcc.download.server.service.DownloadFileSystemService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

@Slf4j
public class ReleaseView extends AbstractDownloadFileSystem {

  public static final String SUMMARY_FILES = "summary_files";

  public ReleaseView(String rootDir, FileSystem fileSystem, @NonNull DownloadFileSystemService fsService) {
    super(rootDir, fileSystem, fsService);
  }

  // Projects
  // README.txt
  // Summary
  public Collection<DownloadFile> listRelease(@NonNull String releaseName) {
    val releaseFiles = HadoopUtils.lsAll(fileSystem, toHdfsPath(releaseName));
    val downloadFiles = releaseFiles.stream()
        .filter(file -> isDfsEntity(file) == false)
        .map(file -> convert2DownloadFile(file))
        .collect(toList());

    downloadFiles.add(createProjectsDir(releaseName));
    downloadFiles.add(createSummaryDir(releaseName));

    return downloadFiles;
  }

  public Collection<DownloadFile> listReleaseProjects(@NonNull String releaseName) {
    val projects = fsService.getReleaseProjects(releaseName);
    val releaseDate = fsService.getReleaseDate(releaseName);

    return projects.stream()
        .map(project -> format("/%s/Projects/%s", releaseName, project))
        .map(path -> createDownloadDir(path, releaseDate))
        .collect(toImmutableList());
  }

  public Collection<DownloadFile> listReleaseSummary(@NonNull String releaseName) {
    val releaseDate = fsService.getReleaseDate(releaseName);
    val clinicalSizes = fsService.getClinicalSizes(releaseName);

    val clinicalFiles = clinicalSizes.entrySet().stream()
        .map(entry -> createSummaryFile(releaseName, entry, releaseDate))
        .collect(toImmutableList());

    val summaryFiles = getSummaryFiles(releaseName);
    log.debug("Summary files: {}", summaryFiles);
    summaryFiles.addAll(clinicalFiles);
    summaryFiles.sort(ReleaseView::sortByPath);

    return ImmutableList.copyOf(summaryFiles);
  }

  public Collection<DownloadFile> listProject(@NonNull String releaseName, @NonNull String project) {
    val releaseDate = fsService.getReleaseDate(releaseName);
    val projectSizes = fsService.getProjectSizes(releaseName, project);

    // No need to sort the output files, as the input is already sorted
    return projectSizes.entrySet().stream()
        .map(entry -> createProjectFile(entry, releaseName, project, releaseDate))
        .collect(toImmutableList());

  }

  private DownloadFile createProjectFile(Entry<DownloadDataType, Long> entry, String releaseName, String project,
      long releaseDate) {
    val type = entry.getKey();
    val name = format("%s.%s.tsv.gz", type.getId(), project);
    val path = format("/%s/Projects/%s/%s", releaseName, project, name);
    val size = entry.getValue();

    return createDownloadFile(path, size, releaseDate);
  }

  private List<DownloadFile> getSummaryFiles(String releaseName) {
    val files = HadoopUtils.lsFile(fileSystem, getSummaryFilesPath(releaseName));

    return files.stream()
        .map(file -> createSummaryFile(file, releaseName))
        .collect(toList());
  }

  private DownloadFile createSummaryFile(Path file, String releaseName) {
    val fileName = file.getName();
    val path = format("/%s/Summary/%s", releaseName, fileName);
    val status = getFileStatus(file);
    val size = status.getLen();
    val creationDate = status.getModificationTime();

    return createDownloadFile(path, size, creationDate);
  }

  private Path getSummaryFilesPath(String releaseName) {
    val summaryPath = new Path(PATH.join(rootDir, releaseName, SUMMARY_FILES));
    log.debug("'{}' summary path: {}", releaseName, summaryPath);

    return summaryPath;
  }

  private DownloadFile createSummaryDir(String releaseName) {
    return createDownloadDir(format("/%s/Summary", releaseName), releaseName);
  }

  private DownloadFile createProjectsDir(String releaseName) {
    return createDownloadDir(format("/%s/Projects", releaseName), releaseName);
  }

  private DownloadFile createSummaryFile(String releaseName, Entry<DownloadDataType, Long> entry, long releaseDate) {
    val type = entry.getKey();
    val size = entry.getValue();
    val fileName = format("%s.all_projects.tsv.gz", type.getId());
    val path = format("/%s/Summary/%s", releaseName, fileName);

    return createDownloadFile(path, size, releaseDate);
  }

  private static boolean isDfsEntity(Path file) {
    val fileName = file.getName();

    return fileName.equals(DATA_DIR) || fileName.equals(HEADERS_DIR) || fileName.equals(SUMMARY_FILES);
  }

  private static int sortByPath(DownloadFile left, DownloadFile right) {
    val leftPath = left.getName();
    val rightPath = right.getName();

    return Ordering.natural().compare(leftPath, rightPath);
  }

}
