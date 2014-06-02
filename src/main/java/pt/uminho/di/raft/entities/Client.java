/*******************************************************************************
 * Copyright (c) 2014 Filipe Campos.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package pt.uminho.di.raft.entities;

import java.io.Console;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.service.reference.DeviceReference;
import pt.uminho.di.raft.service.log.LogEntry;

public class Client extends BasicClient {

    final static Logger logger = Logger.getLogger(Client.class);
    private boolean running = true;
    final static String prompt = "\n\nInsert option number:\n1.\tInsert command\n2.\tRead data\n3.\tQuit\n";
    final static String command_prompt = "Insert command:\n";
    final static String parameters_prompt = "Insert parameters:\n";
    final static String read_prompt = "Insert option number:\n1.\tSpecific entry\n2.\tLast\n";
    final static String entry_prompt = "Insert entry UID:\n";
    Console c = System.console();

    @Override
    public void execute() {
        if (c != null) {
            c.printf("Known leader EPR is %s\n", currentLeader.getAddress());
            while (running) {
                try
                {
                    String option = c.readLine(prompt);
                    logger.debug("Received the following option " + option);
                    if((c != null) && running && !option.isEmpty())
                    {
                        int op = Integer.parseInt(option);
                        switch (op) {
                            case 1:
                                insertCommand(c);
                                break;
                            case 2:
                                readData(c);
                                break;
                            case 3:
                                c.printf("Terminating client...\n");
                                terminate(false);
                                break;
                        }
                    }
                } 
                catch (Exception ex)
                {
                    logger.error(ex.getMessage(), ex);
                }
            }
        } else {
            logger.error("Couldn't instantiate the system's console! Terminating...");
        }
    }

    private void insertCommand(Console c) {
        String command = c.readLine(command_prompt);
        logger.debug("Received the following command " + command);

        String params = c.readLine(parameters_prompt);
        logger.debug("Received the following parameters " + params);

        // invoke insert command
        LogEntry entry = invokeInsertCommandOp(command, params);
        c.printf("Invoked insert command and received %s\n", entry.toShortString());
    }

    private void readData(Console c) {
        String option = c.readLine(read_prompt);
        logger.debug("Received the following option " + option);

        int op = Integer.parseInt(option);
        String uid = null;
        switch (op) {
            
            case 1:
                // get specific entry
                uid = c.readLine(entry_prompt);
                c.printf("Retrieving the entry with UID %s\n", uid);
            case 2:
                // get last entry
                LogEntry entry = invokeReadOp(uid);
                if(entry != null)
                {
                    if(entry.getSuccess())
                        c.printf("Retrieved %s\n", entry);
                    else
                        c.printf("Received %s\n", entry.getResponse());
                }
                else
                {
                    c.printf("Entry is null!\n");
                }
                break;
        }
    }

    @Override
    public void deviceBye(DeviceReference deviceRef) {
        logger.debug(deviceRef.getEndpointReference() + " - Device has sent bye message!");
        c.printf("Current leader has sent bye message. Terminating...\n");
        terminate(true);
    }

    private void terminate(boolean interrupt)
    {
        if(interrupt)
            System.err.println("Press Enter to exit.");
        
        running = false;
        c.flush();
        CoreFramework.stopIgnoringInstancesCount();
        System.exit(0);
    }

    public static void main(String[] args) {
        // configure loggers
        PropertyConfigurator.configure("log4j.properties");

        // mandatory: Starting the DPWS Framework.
        CoreFramework.start(args);
        org.ws4d.java.util.Log.setLogLevel(org.ws4d.java.util.Log.DEBUG_LEVEL_ERROR);

        // Create client...
        Client client = new Client();

        client.searchRaftDevices();
    }
}
