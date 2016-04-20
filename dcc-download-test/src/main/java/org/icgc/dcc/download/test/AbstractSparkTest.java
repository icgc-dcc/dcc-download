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
package org.icgc.dcc.download.test;

import java.io.File;

import lombok.SneakyThrows;
import lombok.val;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.icgc.dcc.download.test.io.TestFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractSparkTest {

	/**
	 * Constants.
	 */
	protected static final String TEST_FIXTURES_DIR = "src/test/resources/fixtures";
	protected static final String INPUT_TEST_FIXTURES_DIR = TEST_FIXTURES_DIR + "/input";
	protected static final String OUTPUT_TEST_FIXTURES_DIR = TEST_FIXTURES_DIR + "/output";

	/**
	 * Collaborators.
	 */
	protected JavaSparkContext sparkContext;
	protected FileSystem fileSystem;
	protected File workingDir;

	/**
	 * State.
	 */
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Before
	@SneakyThrows
	public void setUp() {
		val sparkConf = new SparkConf().setAppName("test").setMaster("local");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.task.maxFailures", "0");

		this.sparkContext = new JavaSparkContext(sparkConf);
		this.fileSystem = FileSystem.getLocal(new Configuration());
		this.workingDir = tmp.newFolder("working");
	}

	@After
	public void tearDown() {
		sparkContext.close();
		sparkContext = null;
	}

	protected void prepareInput() {
		val srcDir = new File(INPUT_TEST_FIXTURES_DIR);
		val destDir = workingDir;
		TestFiles.copyDirectory(srcDir, destDir);
	}

}
