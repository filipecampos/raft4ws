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

import org.ws4d.java.authorization.AuthorizationException;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.client.DefaultClient;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.communication.DPWSCommunicationManager;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.security.SecurityKey;
import org.ws4d.java.service.Device;
import org.ws4d.java.service.Operation;
import org.ws4d.java.service.Service;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.service.reference.DeviceReference;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.HelloData;
import org.ws4d.java.types.QNameSet;
import org.ws4d.java.types.SearchParameter;
import org.ws4d.java.types.URI;
import org.ws4d.java.util.IDGenerator;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.service.log.LogEntry;
import pt.uminho.di.raft.service.operations.InsertCommandOperation;
import pt.uminho.di.raft.service.operations.ReadOperation;

public class BasicClient extends DefaultClient {

    static Logger logger = Logger.getLogger(BasicClient.class);
    protected HashMap<EndpointReference, Operation> devicesInsertCommandOp = new HashMap<EndpointReference, Operation>();
    protected HashMap<EndpointReference, Operation> devicesReadOp = new HashMap<EndpointReference, Operation>();
    protected EndpointReference currentLeader;
    protected InsertCommandOperation insertCommandOp;
    protected ReadOperation readOp;
    private Integer period;
    private Integer num;

    public BasicClient() {
        super();

        // register hello listening
        registerHelloListening();

        insertCommandOp = new InsertCommandOperation(null);
        readOp = new ReadOperation(null);
    }

    public BasicClient(Integer p, Integer n) {
        this();
        period = p;
        num = n;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Integer getPeriod() {
        return period;
    } 

    public void setPeriod(Integer period) {
        this.period = period;
    }

    public void searchRaftDevices() {
        SearchParameter search = new SearchParameter();
        search.setDeviceTypes(new QNameSet(Constants.RaftDeviceQName), DPWSCommunicationManager.COMMUNICATION_MANAGER_ID);
        searchDevice(search);
    }
    
    @Override
    public void helloReceived(HelloData helloData) {

        logger.info("Received hello " + helloData);
        EndpointReference epr = helloData.getEndpointReference();

        // maybe we are already listening to this device. Then we can ignore
        // this hello message...
        if (!devicesInsertCommandOp.containsKey(epr)) {

            // we will get the device reference and by doing this we will tell
            // the device that we are interested and want to listen to status
            // changes
            DeviceReference deviceReference = getDeviceReference(helloData);

            insertNewDevice(epr, deviceReference);

            setFirstLeader(epr);
        }
    }

    @Override
    public void deviceRunning(DeviceReference deviceRef) {
        logger.debug(deviceRef.getEndpointReference() + " - Device running.");
    }

    @Override
    public void deviceChanged(DeviceReference deviceRef) {
        logger.debug(deviceRef.getEndpointReference() + " - Device changed to metadata version.");
    }

    @Override
    public void deviceBuiltUp(DeviceReference deviceRef, Device device) {
        logger.debug(deviceRef.getEndpointReference() + " - Device built up");
    }

    @Override
    public void deviceBye(DeviceReference deviceRef) {
        logger.debug(deviceRef.getEndpointReference() + " - Device has sent bye message");
    }

    @Override
    public void deviceCommunicationErrorOrReset(DeviceReference deviceRef) {
        logger.debug("An error occured or device has been reset.");
    }

    @Override
    public void deviceFound(DeviceReference dr, SearchParameter sp, String string) {
        logger.debug("Device Found: DeviceReference=" + dr + "; SearchParameter=" + sp + "; String=" + string);
        EndpointReference epr = dr.getEndpointReference();

        if (!devicesInsertCommandOp.containsKey(epr)) {
            insertNewDevice(epr, dr);
        }

        setFirstLeader(epr);
    }

    private void setFirstLeader(EndpointReference epr) {
        logger.debug("Setting first leader as " + epr);
        if (currentLeader == null) {
            // set first known server as leader
            currentLeader = epr;
            logger.info("First leader is set!");

            // start inserting commands in its log
            execute();
        }
    }

    protected void insertNewDevice(EndpointReference epr, DeviceReference deviceReference) {
        logger.debug("Inserting new device...");

        try {
            // build up device (otherwise device is running but not built
            // up)
            Device newDvc = deviceReference.getDevice();
            // add it to our set so we can look up if we already know it
            try {
                URI raftSvcIdUri = new URI(Constants.RaftServiceName);
                Service svc = newDvc.getServiceReference(raftSvcIdUri, SecurityKey.EMPTY_KEY).getService();

                if (svc != null) {
                    logger.info("Got Raft Service for device " + epr);
                    // insertCommandOp
                    Operation newInsertCommandOp = svc.getOperation(Constants.RaftServiceQName, Constants.InsertCommandOperationName, null, null);

                    if (newInsertCommandOp != null) {
                        devicesInsertCommandOp.put(epr, newInsertCommandOp);
                        logger.debug("Inserted device " + epr + " InsertCommandOp!");
                    }

                    // readOp
                    Operation newReadOp = svc.getOperation(Constants.RaftServiceQName, Constants.ReadOperationName, null, null);

                    if (newReadOp != null) {
                        devicesReadOp.put(epr, newReadOp);
                        logger.debug("Inserted device " + epr + " ReadOp!");
                    }
                }
            } catch (CommunicationException ex) {
                logger.error(ex.getMessage(), ex);
            }
        } catch (CommunicationException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public LogEntry invokeInsertCommandOp(String command, String parameters) {
        String uid = IDGenerator.getUUID();
        ParameterValue pv = insertCommandOp.getRequestPV(uid, command, parameters);
        LogEntry entry = new LogEntry(uid, -1, -1, command, parameters);

        try {
            if (currentLeader != null) {
                Operation leaderOp = devicesInsertCommandOp.get(currentLeader);
                logger.debug("Invoking Request PV: " + pv);
                ParameterValue response = leaderOp.invoke(pv, CredentialInfo.EMPTY_CREDENTIAL_INFO);
                logger.debug("Received Response PV: " + response);
                String responseSuccessStr = ParameterValueManagement.getString(response, Constants.SuccessElementName);
                Boolean responseSuccess = Boolean.parseBoolean(responseSuccessStr);
                entry.setSuccess(responseSuccess);

                String result = "";
                if (response.getChildrenCount(Constants.ResultElementName) > 0) {
                    result = ParameterValueManagement.getString(response, Constants.ResultElementName);
                    entry.setResponse(result);
                }

                logger.debug("Invoked with success? " + result);
                if (!responseSuccess) {
                    String responseLeader = setLeader(response);
                    if((responseLeader != null) && (!responseLeader.isEmpty()))
                        entry.setResponse(result + ":" + responseLeader);
                } else {
                    // success
                    logger.debug("Invoked with success! result: " + result);
                }

            }
        } catch (InvocationException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (CommunicationException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (AuthorizationException ex) {
            logger.error(ex.getMessage(), ex);
        }

        return entry;
    }

    public LogEntry invokeReadOp(String uid)
    {
        LogEntry entry = null;
        String command = null;
        Object params = null;
        String receivedUid = null;
        Integer term = -1;
        Integer index = -1;

        ParameterValue pv = readOp.getRequestPV(uid);
        try {
            if (currentLeader != null) {
                Operation leaderOp = devicesReadOp.get(currentLeader);
                logger.debug("Invoking Request PV: " + pv);
                ParameterValue response = leaderOp.invoke(pv, CredentialInfo.EMPTY_CREDENTIAL_INFO);
                logger.debug("Received Response PV: " + response);

                // get success
                String responseSuccessStr = ParameterValueManagement.getString(response, Constants.SuccessElementName);
                Boolean responseSuccess = Boolean.parseBoolean(responseSuccessStr);
                // get result
                String responseResult = ParameterValueManagement.getString(response, Constants.ResultElementName);
                
                if(responseSuccess)
                {
                    // get all params
                    receivedUid = ParameterValueManagement.getString(response, Constants.UidElementName);
                    command = ParameterValueManagement.getString(response, Constants.CommandElementName);
                    params = ParameterValueManagement.getString(response, Constants.ParametersElementName);
                    index = Integer.parseInt(ParameterValueManagement.getString(response, Constants.IndexElementName));
                    term = Integer.parseInt(ParameterValueManagement.getString(response, Constants.TermElementName));
                }
                else
                {
                    String responseLeader = setLeader(response);
                    if((responseLeader != null) && (!responseLeader.isEmpty()))
                        responseResult = responseLeader;
                }
                entry = new LogEntry(receivedUid, index, term, command, params);


                entry.setResponse(responseResult);
                entry.setSuccess(responseSuccess);
            }
        } catch (InvocationException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (CommunicationException ex) {
            logger.error(ex.getMessage(), ex);
        } catch (AuthorizationException ex) {
            logger.error(ex.getMessage(), ex);
        }

        return entry;
    }

    protected void setLeaderService(EndpointReference epr)
    {
        if (!devicesInsertCommandOp.containsKey(epr)) {
            logger.debug("New leader with epr: " + epr + " is unknown!");
            try {
                Service svc = this.getServiceReference(epr, DPWSCommunicationManager.COMMUNICATION_MANAGER_ID).getService();

                Operation newInsertCommandOp = svc.getOperation(Constants.RaftServiceQName, Constants.InsertCommandOperationName, null, null);

                if (newInsertCommandOp != null) {
                    devicesInsertCommandOp.put(epr, newInsertCommandOp);
                    logger.debug("Inserted device " + epr + " InsertCommandOp!");
                } else {
                    logger.warn("Did not insert device " + epr + " InsertCommandOp!");
                }

                // readOp
                Operation newReadOp = svc.getOperation(Constants.RaftServiceQName, Constants.ReadOperationName, null, null);

                if (newReadOp != null) {
                    devicesReadOp.put(epr, newReadOp);
                    logger.debug("Inserted device " + epr + " ReadOp!");
                } else {
                    logger.warn("Did not insert device " + epr + " ReadOp!");
                }

            } catch (CommunicationException ex) {
                logger.error(ex.getMessage(), ex);
            }
        }

        if (devicesInsertCommandOp.containsKey(epr)) {
            currentLeader = epr;
            logger.debug("New leader with epr: " + epr + " is now set.");
        } else {
            logger.error("Couldn't retrieve new leader with epr: " + epr);
        }
    }

    protected void execute() {
        String command = "";
        String parameters = "";

        logger.info("Starting for loop...");
        try {
            for (int i = 0; i < num; i++) {
                logger.debug("Invoking " + i + " iteration...");
                // invoke insert command
                command = "x=" + i;
                parameters = "y,z";
                invokeInsertCommandOp(command, parameters);

                // sleep
                Thread.sleep(period);
            }
        } catch (InterruptedException ex) {
            logger.error(ex.getMessage(), ex);
        }
        logger.info("Finished for loop!");

    }

    @Override
    public void finishedSearching(int i, boolean bln, SearchParameter sp) {
        super.finishedSearching(i, bln, sp);
        logger.debug("Finished searching!" + sp);
    }

    private String setLeader(ParameterValue response) {
        String leader = null;
        if (response.getChildrenCount(Constants.LeaderAddressElementName) > 0) {
            // invoked a follower server. retrieving leader epr...
            leader = ParameterValueManagement.getString(response, Constants.LeaderAddressElementName);
            EndpointReference responseLeader = new EndpointReference(new URI(leader));
            logger.info("Got leader epr: " + leader);

            // set new leader
            setLeaderService(responseLeader);
            logger.info("New leader with epr: " + leader + " is now set.");
        }

        return leader;
    }

    public static void main(String[] args) {
        // configure loggers
        PropertyConfigurator.configure("log4j.properties");

        Integer num = 10;
        Integer period = 5000;

        switch (args.length) {
            case 2: {
                period = Integer.parseInt(args[0]);
                num = Integer.parseInt(args[1]);
            }
        }

        // mandatory: Starting the DPWS Framework.
        CoreFramework.start(args);

        // Create client...
        BasicClient client = new BasicClient(period, num);

        org.ws4d.java.util.Log.setLogLevel(org.ws4d.java.util.Log.DEBUG_LEVEL_ERROR);

        client.searchRaftDevices();
    }

}
