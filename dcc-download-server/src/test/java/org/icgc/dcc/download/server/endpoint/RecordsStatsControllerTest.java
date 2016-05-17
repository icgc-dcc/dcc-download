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
package org.icgc.dcc.download.server.endpoint;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.core.model.DownloadDataType.DONOR;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.util.Collection;

import lombok.val;

import org.icgc.dcc.download.server.service.RecordStatsService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class RecordsStatsControllerTest {

  private static final String ENDPOINT_PATH = "/stats";

  @Mock
  RecordStatsService recordStatsService;

  @InjectMocks
  RecordsStatsController controller;

  @Captor
  private ArgumentCaptor<Collection<String>> captor;

  MockMvc mockMvc;

  @Before
  public void setUp() {
    mockMvc = standaloneSetup(controller).build();
  }

  @Test
  public void testEstimateRecordsSizes() throws Exception {
    when(recordStatsService.getRecordsSizes(ImmutableList.of("DO1", "DO2"))).thenReturn(singletonMap(DONOR, 1L));

    mockMvc.perform(get(ENDPOINT_PATH).param("id", "DO1,DO2"))
        .andExpect(status().isOk())
        .andExpect(content().string("{\"sizes\":{\"DONOR\":1}}"));

    verify(recordStatsService).getRecordsSizes(captor.capture());
    val request = captor.getValue();
    assertThat(request).containsOnly("DO1", "DO2");
  }

  @Test
  public void testEstimateRecordsSizes_empty() throws Exception {
    mockMvc.perform(get(ENDPOINT_PATH).param("id", ""))
        .andExpect(status().isBadRequest());
  }

}
