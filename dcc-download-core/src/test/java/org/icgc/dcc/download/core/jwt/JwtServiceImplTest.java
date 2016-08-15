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
package org.icgc.dcc.download.core.jwt;

import static org.assertj.core.api.Assertions.assertThat;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.core.jwt.exception.ExpiredJwtTokenException;
import org.icgc.dcc.download.core.jwt.exception.InvalidJwtTokenException;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class JwtServiceImplTest {

  private static final String AES_KEY = "0123456789123456";
  private static final String SECRET = "01234567891234560123456789123456789012345";

  private static final String EXPIRED_TOKEN =
      "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMTI4R0NNIiwiYWxnIjoiZGlyIn0..S0a3DeSfeT2GEAKj.j_YV5Uw7EI--fCgXMsGD3XMaQAxkoOmP61JVSd4KljyamO0QerRxO5sLzndlXcA6vxcwsFG9gxIjDPG4UYJGOB6WlkQnwFK_-2cmqEeQNzMZvNgwUu3_tR-6K6NWUa__xFFUR6DvffLIomgG9Py4ZnE9DZJ73tFEKsKfY6NxFk6kTMncTseTXmJP2cB9cjgersnX6kR9VCcRfE0WlYk3jRK_JYCrYlH0GtfC3KVrhw2h-DT6Wk5DKPn1V5g1QYrb2BxN0QAZHtJm8jOEwtX_WKNYP2ipBNQmD9St-I4-Rw.-FbSEFDOFVbcG-kjkz_faQ";

  JwtServiceImpl service;

  @Before
  public void setUp() {
    val config = createConfig();
    service = new JwtServiceImpl(config);

  }

  @Test
  public void integrationTest() {
    val token = service.createToken("zzz123", "ollie.operator");
    log.info("Token: {}", token);
    val tokenPayload = service.parseToken(token);

    log.info("Payload: {}", tokenPayload);

    assertThat(tokenPayload.getId()).isEqualTo("zzz123");
    assertThat(tokenPayload.getUser()).isEqualTo("ollie.operator");
    assertThat(tokenPayload.getPath()).isNull();
  }

  @Test
  public void integrationTest_path() {
    val token = service.createToken("/some/download/path");
    log.info("Token: {}", token);
    val tokenPayload = service.parseToken(token);

    log.info("Payload: {}", tokenPayload);

    assertThat(tokenPayload.getId()).isNull();
    assertThat(tokenPayload.getUser()).isNull();
    assertThat(tokenPayload.getPath()).isEqualTo("/some/download/path");
  }

  @Test(expected = InvalidJwtTokenException.class)
  public void invalidTokenTest_sharedKey() {
    val token = service.createToken("zzz123", "ollie.operator");

    val config = createConfig("O4wq75rPWWpm59cFBe90MiZF8iS_fake");
    service = new JwtServiceImpl(config);
    service.parseToken(token);
  }

  @Test(expected = InvalidJwtTokenException.class)
  public void invalidTokenTest_ttl() {
    val token = service.createToken("zzz123", "ollie.operator");
    val config = createConfig(SECRET, 2);
    service = new JwtServiceImpl(config);

    service.parseToken(token);
  }

  @Test(expected = ExpiredJwtTokenException.class)
  public void expiredTokenTest() {
    service.parseToken(EXPIRED_TOKEN);
  }

  private static JwtConfig createConfig() {
    return createConfig(SECRET, 1);
  }

  private static JwtConfig createConfig(String secret) {
    return createConfig(secret, 1);
  }

  private static JwtConfig createConfig(String secret, int ttlHours) {
    return new JwtConfig(secret, AES_KEY, ttlHours);
  }

}
