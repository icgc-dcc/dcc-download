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

import static org.icgc.dcc.download.server.model.Export.DATA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import lombok.val;

import org.icgc.dcc.download.server.model.ExportFile;
import org.icgc.dcc.download.server.model.MetadataResponse;
import org.icgc.dcc.download.server.service.ExportsService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(MockitoJUnitRunner.class)
public class ExportsControllerTest {

  private static final String ENDPOINT_PATH = "/exports";

  @Mock
  ExportsService exportsService;

  @InjectMocks
  ExportsController controller;

  MockMvc mockMvc;

  @Before
  public void setUp() {
    mockMvc = standaloneSetup(controller).build();
  }

  @Test
  public void testListMetadata() throws Exception {
    val exportFile = new ExportFile("url1", "id1", DATA, 123);
    val metadata = new MetadataResponse(exportFile);
    when(exportsService.getMetadata("http://localhost")).thenReturn(metadata);

    mockMvc
        .perform(get(ENDPOINT_PATH))
        .andExpect(status().isOk())
        .andExpect(content().string("[{"
            + "\"url\":\"url1\","
            + "\"id\":\"id1\","
            + "\"type\":\"DATA\","
            + "\"date\":123"
            + "}]"));
  }

}
