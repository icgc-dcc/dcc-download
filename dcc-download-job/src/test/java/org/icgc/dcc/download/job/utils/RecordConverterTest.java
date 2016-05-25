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
package org.icgc.dcc.download.job.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Joiners.TAB;
import static org.icgc.dcc.download.job.util.TestRows.createExposureSchema;
import static org.icgc.dcc.download.job.util.TestRows.createRow;
import lombok.val;

import org.icgc.dcc.download.core.model.DownloadDataType;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class RecordConverterTest {

  @Test
  public void testConvert() throws Exception {
    val exposureRow =
        createRow(createExposureSchema(), "alco_hist", "alco_hist_int", 1, "exp_notes", "exp_type", null, 2);
    val resolvedValues = ImmutableMap.of(
        "_donor_id", "DO1",
        "_project_id", "DCC-TEST",
        "donor_id", "DID123");

    val converter = new RecordConverter(DownloadDataType.DONOR_EXPOSURE.getDownloadFields());
    val actualValue = converter.convert(resolvedValues, exposureRow);
    val expectedValue = TAB.join("DO1", "DCC-TEST", "DID123", "exp_type", 1, "", 2, "alco_hist",
        "alco_hist_int");
    assertThat(actualValue).isEqualTo(expectedValue);
  }

}
