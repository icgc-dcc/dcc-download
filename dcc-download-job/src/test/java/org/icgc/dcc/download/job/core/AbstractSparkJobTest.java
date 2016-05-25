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
package org.icgc.dcc.download.job.core;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.job.task.TaskContext;
import org.icgc.dcc.download.job.util.FileTests;
import org.icgc.dcc.download.test.AbstractSparkTest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@Slf4j
public abstract class AbstractSparkJobTest extends AbstractSparkTest {

  protected static final String JOB_ID = "job123";

  protected JobContext createJobContext(String jobId, Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    return new JobContext(
        jobId,
        donorIds,
        dataTypes,
        sparkContext,
        fileSystem,
        INPUT_TEST_FIXTURES_DIR,
        workingDir.getAbsolutePath());
  }

  protected TaskContext createTaskContext(String jobId, Set<String> donorIds, Set<DownloadDataType> dataTypes) {
    return TaskContext.builder()
        .jobId(jobId)
        .inputDir(workingDir.getAbsolutePath())
        .outputDir(workingDir.getAbsolutePath())
        .donorIds(donorIds)
        .dataTypes(dataTypes)
        .sparkContext(sparkContext)
        .build();
  }

  protected List<File> getDataTypeDirs() {
    val jobDir = new File(workingDir, JOB_ID);
    File[] files = jobDir.listFiles(f -> f.isDirectory());

    return ImmutableList.copyOf(files);
  }

  protected String getExpectedFile(DownloadDataType dataType) {
    return OUTPUT_TEST_FIXTURES_DIR + "/" + dataType.getId() + ".tsv.gz";
  }

  protected void prepareVerificationFiles() {
    getDataTypeDirs().stream()
        .forEach(dir -> FileTests.concatenatePartFiles(dir));

  }

  protected void verifyDataTypeOutput(DownloadDataType dataType) {
    log.info("Verifyging {}", dataType);
    val expectedFile = getExpectedFile(dataType);
    val actualFile = getActualFile(dataType);
    FileTests.compareFiles(expectedFile, actualFile);
  }

  protected String getActualFile(DownloadDataType dataType) {
    val jobDir = new File(workingDir, JOB_ID);
    val dataTypeDir = new File(jobDir, dataType.getId());

    return new File(dataTypeDir, dataType.getId() + ".gz").getAbsolutePath();
  }

  protected TaskContext createTaskContext(DownloadDataType dataType) {
    return createTaskContext(JOB_ID, ImmutableSet.of("DO001", "DO002"),
        Collections.singleton(dataType));
  }

}
