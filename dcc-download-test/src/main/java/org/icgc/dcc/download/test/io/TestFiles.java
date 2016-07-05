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
package org.icgc.dcc.download.test.io;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;

import java.io.File;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import com.google.common.io.Files;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class TestFiles {

	@SneakyThrows
	public static void copyDirectory(@NonNull File srcDir, @NonNull File destDir) {
		checkState(srcDir.exists(), "%s does not exists.", srcDir);
		checkState(srcDir.isDirectory(), "%s exists, but not a directory.", srcDir);
		ensureExistence(destDir);

		File[] dirFiles = srcDir.listFiles();
		checkNotNull(dirFiles);
		for (val file : dirFiles) {
			val outputFile = getOutputFile(file, destDir);
			if (file.isDirectory()) {
				copyDirectory(file, outputFile);
			} else {
				log.debug("Copying {} to {} ...", file, outputFile);
				Files.copy(file, outputFile);
			}
		}
	}

	private static File getOutputFile(File file, File destDir) {
		return new File(destDir, file.getName());
	}

	private static void ensureExistence(File destDir) {
		if (destDir.exists() == false && destDir.mkdirs() == false) {
			log.warn("Failed to create directory: {}", destDir);
		}
	}

}
