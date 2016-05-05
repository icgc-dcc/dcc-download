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
package org.icgc.dcc.download.server.mail;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.icgc.dcc.common.core.util.Separators.SLASH;
import static org.springframework.util.MimeTypeUtils.IMAGE_PNG_VALUE;

import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.config.Properties.MailProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import com.google.common.collect.ImmutableMap;

/**
 * See http://www.thymeleaf.org/doc/articles/springmail.html
 */
@Slf4j
@Service
public class Mailer {

  /**
   * Dependencies.
   */
  private final JavaMailSender mailSender;
  private final TemplateEngine templateEngine;
  private final String baseUrl;

  @Autowired
  public Mailer(JavaMailSender mailSender, TemplateEngine templateEngine, MailProperties mail) {
    this.mailSender = mailSender;
    this.templateEngine = templateEngine;
    val portalUrl = mail.getPortalUrl();
    this.baseUrl = portalUrl.endsWith(SLASH) ? portalUrl : portalUrl + SLASH;
  }

  private static final String SUBJECT_PREFIX = "ICGC Portal Notification: ";
  private static final String START_MESSAGE_TEMPLATE_NAME = "submitted-message";
  private static final String SUCCESSFUL_MESSAGE_TEMPLATE_NAME = "successful-message";
  private static final String FAILED_MESSAGE_TEMPLATE_NAME = "failed-message";
  private static final Resource ICGC_LOGO = new ClassPathResource("/templates/icgc-logo-no-text.png");
  private static final Map<String, String> MAIL_SUBJECTS = ImmutableMap.of(
      START_MESSAGE_TEMPLATE_NAME, "Data Download Request",
      SUCCESSFUL_MESSAGE_TEMPLATE_NAME, "Data Download Request Succeeded",
      FAILED_MESSAGE_TEMPLATE_NAME, "Data Download Request Failed"
      );

  public void sendStart(@NonNull String id, @NonNull String recipient) {
    send(START_MESSAGE_TEMPLATE_NAME, createContextMap(id), recipient);
  }

  public void sendSuccessful(@NonNull String id, @NonNull String recipient) {
    send(SUCCESSFUL_MESSAGE_TEMPLATE_NAME, createContextMap(id), recipient);
  }

  public void sendFailed(@NonNull String id, @NonNull String recipient) {
    send(FAILED_MESSAGE_TEMPLATE_NAME, createContextMap(id), recipient);
  }

  private Map<String, MessageContext> createContextMap(String id) {
    return singletonMap("context", createContext(id));
  }

  private MessageContext createContext(String id) {
    return new MessageContext(id, baseUrl + id);
  }

  @SneakyThrows
  private void send(String templateName, Map<String, ?> variables, String recipient) {
    val body = createBody(templateName, variables);
    val subject = createSubject(templateName);
    val mimeMessage = createMimeMessage(subject, body, recipient);

    log.info("Sending...");
    mailSender.send(mimeMessage);
    log.info("Sent: '{}' to '{}'", mimeMessage.getSubject(), recipient);
  }

  private String createSubject(String templateName) {
    return SUBJECT_PREFIX + MAIL_SUBJECTS.get(templateName);
  }

  private String createBody(String templateName, Map<String, ?> variables) {
    val context = new Context();
    context.setVariables(variables);

    return templateEngine.process(templateName, context);
  }

  private MimeMessage createMimeMessage(String subject, String text, String recipient) throws MessagingException {
    val message = new MimeMessageHelper(mailSender.createMimeMessage(), true, UTF_8.name());
    message.setSubject(subject);
    message.setText(text, true);
    message.setTo(recipient);
    message.addInline("logo", ICGC_LOGO, IMAGE_PNG_VALUE);

    return message.getMimeMessage();
  }

}
