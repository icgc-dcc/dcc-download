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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.server.utils.DfsPaths.*;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class DfsPathsTest {

  @Test
  public void testValidatePath() throws Exception {
    validatePath("/");
    validatePath("/README.txt");
    validatePath("/release_21");
    validatePath("/release_21/README.txt");
    validatePath("/current");
    validatePath("/current/README.txt");
    validatePath("/release_21/Projects");
    validatePath("/release_21/Projects/README.txt");
    validatePath("/release_21/Summary");
    validatePath("/release_21/Summary/README.txt");
    validatePath("/current/Projects/README.txt");
    validatePath("/current/Summary");
    validatePath("/current/Summary/README.txt");
    validatePath("/release_21/Projects/TST-CA");
    validatePath("/release_21/Projects/TST-CA/simple_somatic_mutation.open.ALL-US.tsv.gz");
    validatePath("/release_21/Summary/simple_somatic_mutation.aggregated.vcf.gz");
    validatePath("/current/Summary/sample.all_projects.tsv.gz");
    validatePath("/release_26/Projects/LICA-FR/ClinicalSupplemental.xlsx.gz");
  }

  @Test
  public void testIsRealEntity() throws Exception {
    assertThat(isRealEntity("/README.txt")).isTrue();
    assertThat(isRealEntity("/release_21/README.txt")).isTrue();
    assertThat(isRealEntity("/current/Summary/README.txt")).isTrue();
    assertThat(isRealEntity("/release_21/Summary/simple_somatic_mutation.aggregated.vcf.gz")).isTrue();
    assertThat(isRealEntity("/release_21/Summary/sample.all_projects.tsv.gz")).isFalse();
    assertThat(isRealEntity("/release_26/Projects/LICA-FR/ClinicalSupplemental.xlsx.gz"));
  }

  @Test
  public void testGetDownloadDataType() throws Exception {
    assertThat(getDownloadDataType("/current/Summary/donor.all_projects.tsv.gz")).isEqualTo(DownloadDataType.DONOR);

  }

  @Test
  public void testGetProject() throws Exception {
    assertThat(getProject("/release_21/Projects/AML-US/donor.AML-US.tsv.gz").get()).isEqualTo("AML-US");
    assertThat(getProject("/current/Summary/sample.all_projects.tsv.gz").isPresent()).isFalse();
    assertThat(getProject("/release_26/Projects/LICA-FR/ClinicalSupplemental.xlsx.gz").get()).isEqualTo("LICA-FR");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetLegacyRelease_malformed() throws Exception {
    getLegacyRelease(ImmutableList.of("/"));
  }

  @Test
  public void testGetLegacyRelease() throws Exception {
    assertThat(getLegacyRelease(ImmutableList.of("/", "legacy_release"))).isEqualTo("legacy_release");
  }

}
