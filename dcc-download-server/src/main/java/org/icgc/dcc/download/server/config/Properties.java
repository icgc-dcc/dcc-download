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
package org.icgc.dcc.download.server.config;

import static com.google.common.collect.Maps.newLinkedHashMap;

import java.util.Map;

import lombok.Data;

import org.icgc.dcc.download.core.jwt.JwtConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class Properties {

  @Bean
  @ConfigurationProperties(prefix = "job")
  public JobProperties jobProperties() {
    return new JobProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "download.server")
  public DownloadServerProperties downloadServerProperties() {
    return new DownloadServerProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "hadoop")
  public HadoopProperties hadoopProperties() {
    return new HadoopProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "mail")
  public MailProperties mailProperties() {
    return new MailProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "jwt")
  public JwtConfig jwtConfig() {
    return new JwtConfig();
  }

  @Data
  public static class JobProperties {

    private String inputDir;

  }

  @Data
  public static class DownloadServerProperties {

  }

  @Data
  public static class HadoopProperties {

    private Map<String, String> properties = newLinkedHashMap();

  }

  @Data
  public static class MailProperties {

    private boolean enabled = true;
    private String serviceUrl;
    private String portalUrl;
    private Map<String, String> properties = newLinkedHashMap();

  }

}
