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
package org.icgc.dcc.download.job.task;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Optional.of;
import static org.icgc.dcc.common.core.util.Separators.NEWLINE;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.io.Files2;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.core.AbstractSparkJobTest;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@Slf4j
public class ClinicalTaskTest extends AbstractSparkJobTest {

  private static final String JOB_ID = "job123";

  ClinicalTask task = new ClinicalTask();

  @Test
  public void testExecute() throws Exception {
    prepareInput();
    val taskContext = createTaskContext();
    task.execute(taskContext);

    prepareVerificationFiles();
    verifyResults();
  }

  private void verifyResults() {
    DownloadDataType.CLINICAL.stream()
        .forEach(dt -> verifyDataTypeOutput(dt));
  }

  private void verifyDataTypeOutput(DownloadDataType dataType) {
    log.info("Verifyging {}", dataType);
    val expectedFile = getExpectedFile(dataType);
    val actualFile = getActualFile(dataType);
    compareFiles(expectedFile, actualFile);
  }

  private String getActualFile(DownloadDataType dataType) {
    val jobDir = new File(workingDir, JOB_ID);
    val dataTypeDir = new File(jobDir, dataType.getId());

    return new File(dataTypeDir, dataType.getId() + ".gz").getAbsolutePath();
  }

  private String getExpectedFile(DownloadDataType dataType) {
    return OUTPUT_TEST_FIXTURES_DIR + "/" + dataType.getId() + ".tsv.gz";
  }

  private void prepareVerificationFiles() {
    getDataTypeDirs().stream()
        .forEach(dir -> concatenatePartFiles(dir));

  }

  private static void concatenatePartFiles(File partFilesDir) {
    log.info("Concatenating 'part' files in {} ...", partFilesDir);
    File[] partFiles = partFilesDir.listFiles((dir, name) -> name.startsWith("part-"));

    concatenateFiles(of(getDataTypeFile(partFilesDir).getAbsolutePath()),
        ImmutableList.copyOf(partFiles));
  }

  private static File getDataTypeFile(File dataTypeDir) {
    val dataTypeName = dataTypeDir.getName();

    return new File(dataTypeDir, dataTypeName + ".gz");
  }

  private List<File> getDataTypeDirs() {
    val jobDir = new File(workingDir, JOB_ID);
    File[] files = jobDir.listFiles(f -> f.isDirectory());
    log.debug("Files in {}: {}", jobDir, files);

    return ImmutableList.copyOf(files);

  }

  private TaskContext createTaskContext() {
    return createTaskContext(JOB_ID, ImmutableSet.of("DO001", "DO002"), DownloadDataType.CLINICAL);
  }

  @SneakyThrows
  private static File concatenateFiles(@NonNull Optional<String> outFile, @NonNull List<File> inFiles) {
    if (inFiles.isEmpty()) {
      log.error("The input files list is empty. Skipping files concatenation...");
    }

    val outputFile = outFile.isPresent() ? getFile(outFile.get(), true) : getTempFile();
    log.debug("Concatenating files. Out file: {}. Input files: {}", outputFile, inFiles);
    ensureFileWritability(outputFile);

    @Cleanup
    val outStream = new FileOutputStream(outputFile);
    for (val inFile : inFiles) {
      Files.copy(inFile, outStream);
    }

    return outputFile;
  }

  // TODO: move to commons
  private static void compareFiles(@NonNull String expectedFilePath, @NonNull String actualFilePath) {
    val expectedFile = getFile(expectedFilePath);
    val actualFile = getFile(actualFilePath);
    log.debug("Comparing files. Expected: {}. Actual: {}", expectedFile.getAbsolutePath(), actualFile.getAbsolutePath());

    val sortedExpectedFile = sortFile(expectedFile);
    val sortedActualFile = sortFile(actualFile);
    compareSortedFiles(sortedExpectedFile, sortedActualFile);
  }

  @SneakyThrows
  private static void compareSortedFiles(File expected, File actual) {
    @Cleanup
    val expectedFileReader = Files2.getCompressionAgnosticBufferedReader(expected.getAbsolutePath());
    @Cleanup
    val actualFileReader = Files2.getCompressionAgnosticBufferedReader(actual.getAbsolutePath());
    int lineNumber = 1;
    String expectedLine = null;
    while ((expectedLine = expectedFileReader.readLine()) != null) {
      val actualLine = actualFileReader.readLine();
      log.debug("Comparing line:\n{}\n with \n{}", expectedLine, actualLine);

      if (!expectedLine.equals(actualLine)) {
        throw new AssertionError(getAssersionErrorMessage(lineNumber, expectedLine, actualLine));
      }
      lineNumber++;
    }
  }

  private static String getAssersionErrorMessage(int lineNumber, String expectedLine, String actualLine) {
    return format("Assertion failed at line %s. Expected line:%n%s%n"
        + "Actual line:%n%s", lineNumber, expectedLine, actualLine);
  }

  @SneakyThrows
  private static File sortFile(File sourceFile) {
    val lines = readFileToMutableList(sourceFile);
    Collections.sort(lines);

    val sortedFile = getTempFile();
    @Cleanup
    val sortedFileWriter = getBufferedWriter(sortedFile);
    for (val line : lines) {
      sortedFileWriter.write(line);
      sortedFileWriter.write(NEWLINE);
    }

    return sortedFile;
  }

  @SneakyThrows
  private static List<String> readFileToMutableList(File sourceFile) {
    ensureFileReadability(sourceFile);

    @Cleanup
    val sourceReader = Files2.getCompressionAgnosticBufferedReader(sourceFile.getAbsolutePath());
    val linesList = Lists.<String> newArrayList();
    String line = null;
    while ((line = sourceReader.readLine()) != null) {
      linesList.add(line);
    }

    return linesList;
  }

  @SneakyThrows
  private static File getTempFile() {
    val tempFile = File.createTempFile("test-", "tmp");
    tempFile.deleteOnExit();

    return tempFile;
  }

  private static File getFile(String fileName) {
    return getFile(fileName, false);
  }

  @SneakyThrows
  private static File getFile(String fileName, boolean create) {
    val file = new File(fileName);
    if (create && !file.exists()) {
      file.createNewFile();
    }
    ensureFileReadability(file);

    return file;
  }

  @SneakyThrows
  private static BufferedWriter getBufferedWriter(File file) {
    ensureFileWritability(file);

    return new BufferedWriter(new FileWriter(file));
  }

  private static void ensureFileWritability(File testFile) {
    checkState(testFile.canWrite(), "File '%s' is not writable.", testFile.getAbsolutePath());
  }

  private static void ensureFileReadability(File testFile) {
    checkState(testFile.canRead(), "File '%s' is not readable.", testFile.getAbsolutePath());
  }

}
