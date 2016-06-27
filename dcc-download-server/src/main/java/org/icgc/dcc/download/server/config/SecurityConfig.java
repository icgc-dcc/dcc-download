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

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.authentication.configurers.GlobalAuthenticationConfigurerAdapter;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Slf4j
public class SecurityConfig {

  @Configuration
  @EnableWebSecurity
  @Profile("secure")
  public static class EndpointSecurity extends WebSecurityConfigurerAdapter {

    @Override
    public void configure(HttpSecurity http) throws Exception {
      log.info("Configuring enpoint security...");
      // @formatter:off
      http.requestMatchers()

          // Start security configuration for endpoints matching the patterns
          .antMatchers("/downloads/**", "/list/**", "/srv-info/**")
          .and()

          // Set access permissions for particular endpoints
          .authorizeRequests()
          
          // Allow static and dynamic download to everyone 
          .antMatchers(GET, "/downloads/*").permitAll()
          
          // Require authentication for all the other endpoints
          .antMatchers(GET, "/downloads/*/info").authenticated()
          .antMatchers(POST,"/downloads/**").authenticated()
          .antMatchers("/list/**").authenticated()
          .antMatchers("/srv-info/**").authenticated()
          .and()

          // Require the BASIC authentication for all auth requests
          .httpBasic()
          
          // Disable Cross-Site Request Forgery as each request to the server is independent on the other
          .and().csrf().disable();
      // @formatter:on
    }
  }

  @Order(Ordered.HIGHEST_PRECEDENCE)
  @Configuration
  @Profile("secure")
  public static class AuthenticationSecurity extends GlobalAuthenticationConfigurerAdapter {

    @Value("${download.server.user}")
    private String user;
    @Value("${download.server.password}")
    private String password;
    @Value("${download.server.adminUser}")
    private String adminUser;
    @Value("${download.server.adminPassword}")
    private String adminPassword;

    @Override
    public void init(AuthenticationManagerBuilder auth) throws Exception {
      log.info("Configuring authentication security...");
      // @formatter:off
      auth.inMemoryAuthentication()
        .withUser(adminUser).password(adminPassword).roles("ADMIN", "USER")
        .and()
        .withUser(user).password(password).roles("USER");
      // @formatter:on
    }

  }

}
