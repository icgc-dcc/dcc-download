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
package org.icgc.dcc.download.imports;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.repeat;
import static java.lang.System.err;
import static java.lang.System.out;

import java.util.Optional;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.imports.command.ClientCommand;
import org.icgc.dcc.download.imports.command.IndexClientCommand;
import org.icgc.dcc.download.imports.conf.ClientOptions;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

@Slf4j
public class ClientMain {

  /**
   * Constants.
   */
  private static final String APPLICATION_NAME = "dcc-metadata-client";
  private static final int FAILURE_STATUS_CODE = 1;

  public static void main(String... args) {
    val options = new ClientOptions();
    val cli = new JCommander(options);
    cli.setProgramName(APPLICATION_NAME);

    try {
      cli.parse(args);
      if (options.help) {
        usage(cli);
        return;
      }

      if (options.version) {
        version();
        return;
      }

      if (options.inputFile == null) {
        err.println("The input index file is unset.");
        return;
      }

      if (isNullOrEmpty(options.esUrl)) {
        err.println("Elastisticsearch cluster URL is unset.");
        return;
      }

      execute(options);
    } catch (ParameterException e) {
      log.error("Invalid parameter(s): ", e);
      err.println("Invalid parameter(s): " + e.getMessage());
      usage(cli);
    } catch (Exception e) { // NOPMD
      log.error("Unknown error: ", e);
      err.println("Unknow error. Please check the log for detailed error messages: " + e.getMessage());
      System.exit(FAILURE_STATUS_CODE);
    }
  }

  private static void execute(ClientOptions options) {
    banner("Running with {}", options);

    val command = resolveCommand(options);
    command.execute();
  }

  private static ClientCommand resolveCommand(ClientOptions options) {
    return new IndexClientCommand(
        options.inputFile,
        Optional.ofNullable(options.project),
        TarArchiveEntryCallbackFactory.create(options.esUrl),
        TarArchiveDocumentReaderFactory.create());
  }

  private static void banner(String message, Object... args) {
    log.info("{}", repeat("-", 100));
    log.info(message, args);
    log.info("{}", repeat("-", 100));
  }

  private static void usage(JCommander cli) {
    val message = new StringBuilder();
    cli.usage(message);
    err.println(message.toString());
  }

  private static void version() {
    val version = firstNonNull(ClientMain.class.getPackage().getImplementationVersion(), "[unknown version]");
    out.printf("DCC Download Import Client%nVersion %s%n", version);
  }

}
