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

import static org.icgc.dcc.common.test.json.JsonNodes.$;
import static org.icgc.dcc.download.server.model.Export.DATA_CONTROLLED;
import static org.icgc.dcc.download.server.model.Export.DATA_OPEN;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import lombok.val;

import org.icgc.dcc.download.server.io.FileStreamer;
import org.icgc.dcc.download.server.model.ExportFile;
import org.icgc.dcc.download.server.model.MetadataResponse;
import org.icgc.dcc.download.server.service.AuthService;
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

  private static final String TOKEN = "token1";
  private static final String AUTH_HEADER_VALUE = "authHeaderValue";
  private static final String ENDPOINT_PATH = "/exports";

  @Mock
  ExportsService exportsService;
  @Mock
  AuthService authService;
  @Mock
  FileStreamer fileStreamer;

  @InjectMocks
  ExportsController controller;

  MockMvc mockMvc;

  @Before
  public void setUp() {
    mockMvc = standaloneSetup(controller).build();
  }

  @Test
  public void testListMetadata_open() throws Exception {
    val exportFile = new ExportFile("url1", DATA_OPEN.getId(), DATA_OPEN.getType(), 123);
    val metadata = new MetadataResponse(exportFile);
    when(exportsService.getOpenMetadata("http://localhost")).thenReturn(metadata);

    val expecedBody = $("[{url:'url1',id:'data.open.tar',type:'data',date:123}]");
    mockMvc
        .perform(get(ENDPOINT_PATH))
        .andExpect(status().isOk())
        .andExpect(content().json(expecedBody.toString()));

    verify(exportsService, times(0)).getControlledMetadata(anyString());
  }

  @Test
  public void testListMetadata_controlled() throws Exception {
    val exportFile = new ExportFile("url1", DATA_CONTROLLED.getId(), DATA_CONTROLLED.getType(), 123);
    val metadata = new MetadataResponse(exportFile);
    when(exportsService.getControlledMetadata("http://localhost")).thenReturn(metadata);
    when(authService.parseToken(AUTH_HEADER_VALUE)).thenReturn(TOKEN);
    when(authService.isAuthorized(TOKEN)).thenReturn(true);

    val expecedBody = $("[{url:'url1',id:'data.controlled.tar',type:'data',date:123}]");
    mockMvc
        .perform(get(ENDPOINT_PATH)
            .header("Authorization", AUTH_HEADER_VALUE))
        .andExpect(status().isOk())
        .andExpect(content().json(expecedBody.toString()));
    verify(exportsService, times(0)).getOpenMetadata(anyString());
  }

  @Test
  public void testDownloadArchive_open() throws Exception {
    val exportId = DATA_OPEN.getId();
    when(exportsService.getExportStreamer(eq(DATA_OPEN), any())).thenReturn(fileStreamer);
    when(fileStreamer.getName()).thenReturn(exportId);

    mockMvc
        .perform(get(ENDPOINT_PATH + "/" + exportId))
        .andExpect(status().isOk());
  }

  @Test
  public void testDownloadArchive_controlled() throws Exception {
    val exportId = DATA_CONTROLLED.getId();
    when(exportsService.getExportStreamer(eq(DATA_CONTROLLED), any())).thenReturn(fileStreamer);
    when(fileStreamer.getName()).thenReturn(exportId);
    when(authService.parseToken(AUTH_HEADER_VALUE)).thenReturn(TOKEN);
    when(authService.isAuthorized(TOKEN)).thenReturn(true);

    mockMvc
        .perform(get(ENDPOINT_PATH + "/" + exportId)
            .header("Authorization", AUTH_HEADER_VALUE))
        .andExpect(status().isOk());
  }

  @Test
  public void testDownloadArchive_controlledForbidden() throws Exception {
    val exportId = DATA_CONTROLLED.getId();

    mockMvc
        .perform(get(ENDPOINT_PATH + "/" + exportId))
        .andExpect(status().isForbidden());
  }

  @Test
  public void testDownloadArchive_notFound() throws Exception {
    mockMvc
        .perform(get(ENDPOINT_PATH + "/invalid_id"))
        .andExpect(status().isNotFound());
  }

}
