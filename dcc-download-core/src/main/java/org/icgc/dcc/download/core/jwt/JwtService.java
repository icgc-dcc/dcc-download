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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.common.core.json.Jackson;
import org.icgc.dcc.download.core.jwt.exception.ExpiredJwtTokenException;
import org.icgc.dcc.download.core.jwt.exception.InvalidJwtTokenException;
import org.icgc.dcc.download.core.model.TokenPayload;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.JWEObject;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.DirectDecrypter;
import com.nimbusds.jose.crypto.DirectEncrypter;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

/**
 * http://connect2id.com/products/nimbus-jose-jwt/examples/signed-and-encrypted-jwt
 */
@Slf4j
public class JwtService {

  /**
   * Constants.
   */
  private static final String DOWNLOAD_USER_NAME = "user";
  private static final String DOWNLOAD_ID_NAME = "id";
  private static final String DOWNLOAD_PATH_NAME = "path";

  /**
   * Configuration.
   */
  private static final int AES_KEY_LENGTH = 16;

  /**
   * Header for encrypted tokens.
   */
  private static final JWEHeader JWE_HEADER = createJweHeader();

  /**
   * Header for signed tokens.
   */
  private static final JWSHeader JWS_HEADER = new JWSHeader(JWSAlgorithm.HS256);

  /**
   * Dependencies.
   */
  private final DirectEncrypter encrypter;
  private final DirectDecrypter decrypter;
  private final MACVerifier verifier;
  private final MACSigner signer;

  private final int ttlHours;

  @SneakyThrows
  public JwtService(@NonNull JwtConfig config) {
    val secret = config.getSharedSecret();
    val aesKey = config.getAesKey();

    checkArguments(secret, aesKey);
    this.signer = new MACSigner(secret);
    this.verifier = new MACVerifier(secret);
    this.encrypter = new DirectEncrypter(aesKey.getBytes(UTF_8));
    this.decrypter = new DirectDecrypter(aesKey.getBytes(UTF_8));
    this.ttlHours = config.getTtlHours();
  }

  @SneakyThrows
  public String createToken(@NonNull String downloadId, @NonNull String user) {
    val signedJwt = createSignedJwt(new TokenPayload(downloadId, user, null));
    val encryptedJwt = encryptJwt(signedJwt);

    return encryptedJwt.serialize();
  }

  @SneakyThrows
  public String createToken(@NonNull String path) {
    val signedJwt = createSignedJwt(new TokenPayload(null, null, path));
    val encryptedJwt = encryptJwt(signedJwt);

    return encryptedJwt.serialize();
  }

  @SneakyThrows
  public TokenPayload parseToken(@NonNull String token) {
    val jweObject = JWEObject.parse(token);
    jweObject.decrypt(decrypter);

    val signedJWT = jweObject.getPayload().toSignedJWT();
    val verified = signedJWT.verify(verifier);
    if (verified) {
      val tokenPayload = getPayload(signedJWT);

      return tokenPayload;
    }

    throw new InvalidJwtTokenException(token);
  }

  /**
   * Creates a JWT and signs it.
   */
  @SneakyThrows
  private SignedJWT createSignedJwt(TokenPayload tokenPayload) {
    val claimsSet = new JWTClaimsSet.Builder()
        .jwtID(UUID.randomUUID().toString())
        .issueTime(Date.from(Instant.now()))
        .expirationTime(getExpirationTime())
        .claim(DOWNLOAD_ID_NAME, tokenPayload.getId())
        .claim(DOWNLOAD_USER_NAME, tokenPayload.getUser())
        .claim(DOWNLOAD_PATH_NAME, tokenPayload.getPath())
        .build();

    val signedJWT = new SignedJWT(JWS_HEADER, claimsSet);
    signedJWT.sign(signer);

    return signedJWT;
  }

  /**
   * Creates an encrypted JWE Object where the payload is the {@code jwtToken}.
   */
  @SneakyThrows
  private JWEObject encryptJwt(SignedJWT jwtToken) {
    val jweObject = new JWEObject(JWE_HEADER, new Payload(jwtToken));
    jweObject.encrypt(encrypter);

    return jweObject;
  }

  private Date getExpirationTime() {
    val expiration = Instant.now().plus(ttlHours, ChronoUnit.HOURS);

    return Date.from(expiration);
  }

  @SneakyThrows
  private TokenPayload getPayload(SignedJWT jweObject) {
    val payload = jweObject.getPayload();
    val json = Jackson.DEFAULT.readValue(payload.toBytes(), ObjectNode.class);
    verifyExpiration(json);
    verifyTtl(json);

    return convert(json);
  }

  private void verifyTtl(ObjectNode payload) {
    log.debug("Verifying token TTL configuration. Token: {}", payload);
    val created = getTokenLongValue(payload, "iat");
    val expires = getTokenLongValue(payload, "exp");
    val createdDate = Instant.ofEpochSecond(created);
    val expiresDate = Instant.ofEpochSecond(expires);
    val tokenTtl = Duration.between(createdDate, expiresDate).toHours();
    log.debug("Token TTL hours: {}", tokenTtl);
    if (tokenTtl != ttlHours) {
      log.warn("Token '{}' has invalid TTL. Expected: {} hours. Actual: {} hours", payload, ttlHours, tokenTtl);
      throw new InvalidJwtTokenException();
    }
  }

  private static void verifyExpiration(ObjectNode payload) {
    log.debug("Verifying token for expication. Token: {}", payload);

    checkState(payload.has("exp"), "The token has no expiration date set.");
    val expiration = payload.get("exp").asLong();
    val tokenExpirationDate = Instant.ofEpochSecond(expiration);
    log.debug("Token expiration: {}", tokenExpirationDate);

    if (tokenExpirationDate.isBefore(Instant.now())) {
      log.warn("Token '{}' is expired.", payload);
      throw new ExpiredJwtTokenException();
    }
  }

  private static TokenPayload convert(ObjectNode json) {
    val downloadId = getTokenValue(json, DOWNLOAD_ID_NAME);
    val user = getTokenValue(json, DOWNLOAD_USER_NAME);
    val path = getTokenValue(json, DOWNLOAD_PATH_NAME);

    return new TokenPayload(downloadId, user, path);
  }

  private static void checkArguments(String secret, String aesKey) {
    val aesKeyLength = aesKey.getBytes(UTF_8).length;
    checkArgument(aesKeyLength == AES_KEY_LENGTH, "Expected AES Key length is %s bytes, but got %s", AES_KEY_LENGTH,
        aesKeyLength);
  }

  private static JWEHeader createJweHeader() {
    return new JWEHeader.Builder(JWEAlgorithm.DIR, EncryptionMethod.A128GCM)
        .contentType("JWT") // required to signal nested JWT
        .build();
  }

  private static long getTokenLongValue(ObjectNode json, String key) {
    val value = json.path(key);
    if (value.isMissingNode() || value.isNull()) {
      throw new InvalidJwtTokenException(format("Failed to the '%s' from the token.", key));
    }

    return value.asLong();
  }

  private static String getTokenValue(ObjectNode json, String key) {
    if (json.has(key)) {
      return json.get(key).asText();
    }

    return null;
  }

}
