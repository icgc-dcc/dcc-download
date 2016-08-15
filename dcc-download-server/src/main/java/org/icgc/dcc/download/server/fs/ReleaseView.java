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
import static java.util.Optional.empty;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.checkExistence;
import static org.icgc.dcc.download.core.model.DownloadFileType.DIRECTORY;
import static org.icgc.dcc.download.core.model.DownloadFileType.FILE;
import static org.icgc.dcc.download.server.utils.DfsPaths.getFileName;
import static org.icgc.dcc.download.server.utils.DfsPaths.getProjectPath;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.PROJECTS_FILES;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.SUMMARY_FILES;
import static org.icgc.dcc.download.server.utils.Releases.getActualReleaseName;
import static org.icgc.dcc.download.server.utils.Requests.checkRequestPath;
import static org.icgc.dcc.download.server.utils.Responses.throwBadRequestException;
import static org.icgc.dcc.download.server.utils.Responses.throwPathNotFoundException;

import java.util.List;
import java.util.Map.Entry;

import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.server.endpoint.BadRequestException;
import org.icgc.dcc.download.server.endpoint.NotFoundException;
import org.icgc.dcc.download.server.service.FileSystemService;
import org.icgc.dcc.download.server.utils.DownloadDirectories;

import com.google.common.collect.Lists;

@Slf4j
public class ReleaseView extends AbstractFileSystemView {

  public ReleaseView(FileSystem fileSystem, @NonNull FileSystemService fsService,
      @NonNull PathResolver pathResolver) {
    super(fileSystem, fsService, pathResolver);
  }

  public List<DownloadFile> listRelease(@NonNull String releaseName) {
    val current = "current".equals(releaseName);
    val actualReleaseName = current ? currentRelease.get() : releaseName;
    log.debug("Listing release contents for release '{}'", releaseName);

    val hdfsPath = pathResolver.toHdfsPath("/" + actualReleaseName);
    ensurePathExistence(hdfsPath);

    val releaseFiles = HadoopUtils.lsAll(fileSystem, hdfsPath);
    val downloadFiles = releaseFiles.stream()
        .filter(file -> !isDfsEntity(file))
        .map(file -> convert2DownloadFile(file, current))
        .collect(toList());

    downloadFiles.add(createProjectsDir(releaseName));
    downloadFiles.add(createSummaryDir(releaseName));

    return downloadFiles.stream()
        .sorted()
        .collect(toImmutableList());
  }

  public List<DownloadFile> listReleaseProjects(@NonNull String releaseName) {
    val actualReleaseName = getActualReleaseName(releaseName, currentRelease.get());
    val projectNames = fsService.getReleaseProjects(actualReleaseName);
    if (!projectNames.isPresent()) {
      throwPathNotFoundException(format("Release '%s' doesn't exist.", actualReleaseName));
    }
    val releaseDate = getReleaseDate(actualReleaseName);

    val projects = projectNames.get().stream()
        .map(project -> getProjectPath(releaseName, project))
        .map(path -> createDownloadDir(path, releaseDate))
        .sorted()
        .collect(toImmutableList());

    val projectsFiles = getProjectsFiles(releaseName);
    log.debug("Projects files: {}", projectsFiles);
    projectsFiles.addAll(projects);

    return projectsFiles;
  }

  public List<DownloadFile> listReleaseSummary(@NonNull String releaseName) {
    val actualReleaseName = getActualReleaseName(releaseName, currentRelease.get());
    val releaseDate = getReleaseDate(actualReleaseName);
    val clinicalSizes = fsService.getClinicalSizes(actualReleaseName);

    val clinicalFiles = clinicalSizes.entrySet().stream()
        .map(entry -> createSummaryFile(releaseName, entry, releaseDate))
        .collect(toImmutableList());
    log.debug("Clinical Files: {}", clinicalFiles);

    val summaryFiles = getSummaryFiles(releaseName);
    log.debug("Summary files: {}", summaryFiles);
    summaryFiles.addAll(clinicalFiles);

    return summaryFiles.stream()
        .sorted()
        .collect(toImmutableList());
  }

  public List<DownloadFile> listProject(@NonNull String releaseName, @NonNull String project) {
    val actualReleaseName = getActualReleaseName(releaseName, currentRelease.get());
    val releaseDate = getReleaseDate(actualReleaseName);
    checkRequestPath(fsService.existsProject(actualReleaseName, project), getProjectPath(releaseName, project));
    val projectSizes = fsService.getProjectSizes(actualReleaseName, project);

    // No need to sort the output files, as the input is already sorted
    return projectSizes.entrySet().stream()
        .map(entry -> createProjectFile(entry, releaseName, project, releaseDate))
        .sorted()
        .collect(toImmutableList());

  }

  public List<DownloadFile> listLegacy(String relativePath) {
    val path = pathResolver.toLegacyHdfsPath(relativePath);
    ensureDirectory(path);

    val allFiles = HadoopUtils.lsAll(fileSystem, path);

    return allFiles.stream()
        .map(file -> createDownloadFile(file, pathResolver.toDfsPath(file)))
        .collect(toImmutableList());
  }

  private List<DownloadFile> getProjectsFiles(String releaseName) {
    val actualReleaseName = getActualReleaseName(releaseName, currentRelease.get());
    val projectsFiles = getProjectsFilesPath(actualReleaseName);
    if (!checkExistence(fileSystem, projectsFiles)) {
      return Lists.newArrayList();
    }

    val files = HadoopUtils.lsFile(fileSystem, projectsFiles);

    return files.stream()
        .map(file -> createProjectsFile(file, releaseName))
        .collect(toList());
  }

  private DownloadFile createProjectFile(Entry<DownloadDataType, Long> entry, String releaseName, String project,
      long releaseDate) {
    val type = entry.getKey();
    val name = format("%s.%s.tsv.gz", getFileName(type, empty()), project);
    val path = format("/%s/Projects/%s/%s", releaseName, project, name);
    val size = entry.getValue();

    return createDownloadFile(path, size, releaseDate);
  }

  private List<DownloadFile> getSummaryFiles(String releaseName) {
    val actualReleaseName = getActualReleaseName(releaseName, currentRelease.get());
    val summaryFiles = getSummaryFilesPath(actualReleaseName);
    if (!checkExistence(fileSystem, summaryFiles)) {
      return Lists.newArrayList();
    }

    val files = HadoopUtils.lsFile(fileSystem, summaryFiles);

    return files.stream()
        .map(file -> createSummaryFile(file, releaseName))
        .collect(toList());
  }

  /**
   * Creates a {@link DownloadFile} that represents a real file in the {@code Projects} directory.<br>
   * E.g. {@code /release_21/Projects/README.txt}
   */
  private DownloadFile createProjectsFile(Path file, String releaseName) {
    log.debug("Creating projects file for '{}'", file);
    val fileName = file.getName();
    val path = format("/%s/Projects/%s", releaseName, fileName);

    return createDownloadFile(file, path);
  }

  /**
   * Creates a {@link DownloadFile} that represents a real file in the {@code Summary} directory.<br>
   * E.g. {@code /release_21/Summary/README.txt}
   */
  // TODO: extract common functionality with createProjectsFile()
  private DownloadFile createSummaryFile(Path file, String releaseName) {
    log.debug("Creating summary file for '{}'", file);
    val fileName = file.getName();
    val path = format("/%s/Summary/%s", releaseName, fileName);

    return createDownloadFile(file, path);
  }

  private DownloadFile createDownloadFile(Path file, String downloadFilePath) {
    val status = getFileStatus(file);
    val type = status.isDirectory() ? DIRECTORY : FILE;
    val size = type == FILE ? status.getLen() : 0L;
    val creationDate = status.getModificationTime();

    return createDownloadFile(downloadFilePath, type, size, creationDate);
  }

  private Path getProjectsFilesPath(String releaseName) {
    val summaryPath = new Path(PATH.join(pathResolver.getRootDir(), releaseName, PROJECTS_FILES));
    log.debug("'{}' summary path: {}", releaseName, summaryPath);

    return summaryPath;
  }

  private Path getSummaryFilesPath(String releaseName) {
    val summaryPath = new Path(PATH.join(pathResolver.getRootDir(), releaseName, SUMMARY_FILES));
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

  private void ensurePathExistence(Path hdfsPath) {
    if (!HadoopUtils.exists(fileSystem, hdfsPath)) {
      throwPathNotFoundException(format("File not exists: '%s'", hdfsPath));
    }
  }

  /**
   * Verifies that the {@code hdfsPath} is a directory.
   * @throws NotFoundException if {@code hdfsPath} doesn't exist.
   * @throws BadRequestException if {@code hdfsPath} is not a directory.
   */
  private void ensureDirectory(Path hdfsPath) {
    val statusOpt = HadoopUtils.getFileStatus(fileSystem, hdfsPath);
    if (!statusOpt.isPresent()) {
      val dfsPath = pathResolver.toDfsPath(hdfsPath);
      throwPathNotFoundException(format("File not exists: '%s'", dfsPath));
    }

    val status = statusOpt.get();
    if (!status.isDirectory()) {
      val dfsPath = pathResolver.toDfsPath(hdfsPath);
      throwBadRequestException(format("File is not directory: '%s'", dfsPath));
    }
  }

  /**
   * Checks if the {@code file} represent DFS specific entity. E.g. {@code data} directory
   */
  private static boolean isDfsEntity(Path file) {
    val fileName = file.getName();

    return DownloadDirectories.DOWNLOAD_DIRS.contains(fileName);
  }

}
