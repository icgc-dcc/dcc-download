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

import static java.util.Arrays.stream;
import lombok.NoArgsConstructor;
import lombok.val;

import org.icgc.dcc.download.server.service.AuthService;
import org.icgc.dcc.download.server.service.DccAuthTokenServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.RemoteTokenServices;

@Configuration
@NoArgsConstructor
public class AuthServiceConfig {

  private static final String SECURE_PROFILE_NAME = "secure";

  @Autowired
  Environment env;
  @Value("${auth.server.url}")
  String checkTokenUrl;
  @Value("${auth.server.clientId}")
  String clientId;
  @Value("${auth.server.clientsecret}")
  String clientSecret;

  @Bean
  public AuthService dccAuthTokenService() {
    if (isSecureProfile()) {
      return new DccAuthTokenServiceImpl(remoteTokenServices());
    }

    return new AuthService() {};
  }

  private RemoteTokenServices remoteTokenServices() {
    val remoteTokenServices = new RemoteTokenServices();
    remoteTokenServices.setCheckTokenEndpointUrl(checkTokenUrl);
    remoteTokenServices.setClientId(clientId);
    remoteTokenServices.setClientSecret(clientSecret);
    remoteTokenServices.setAccessTokenConverter(accessTokenConverter());

    return remoteTokenServices;
  }

  private boolean isSecureProfile() {
    return stream(env.getActiveProfiles())
        .anyMatch(profile -> SECURE_PROFILE_NAME.equals(profile));
  }

  private static AccessTokenConverter accessTokenConverter() {
    return new DefaultAccessTokenConverter();
  }

}
