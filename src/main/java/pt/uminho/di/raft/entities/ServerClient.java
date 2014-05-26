package pt.uminho.di.raft.entities;

import java.util.Collection;
import org.ws4d.java.authorization.AuthorizationException;
import pt.uminho.di.raft.service.RaftDeviceInfo;
import java.util.HashMap;
import java.util.Set;
import org.apache.log4j.Logger;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.client.DefaultClient;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.communication.DPWSCommunicationManager;
import org.ws4d.java.security.SecurityKey;
import org.ws4d.java.service.Device;
import org.ws4d.java.service.Service;
import org.ws4d.java.service.reference.DeviceReference;
import org.ws4d.java.structures.Iterator;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.EprInfo;
import org.ws4d.java.types.HelloData;
import org.ws4d.java.types.QName;
import org.ws4d.java.types.QNameSet;
import org.ws4d.java.types.SearchParameter;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.states.CandidateTask;
import pt.uminho.di.raft.entities.workers.ReplicaInvokingTask;
import pt.uminho.di.raft.entities.workers.SearchingTask;
import pt.uminho.di.raft.entities.workers.VoteRequestingTask;

/**
 *
 * @author fcampos
 */
public class ServerClient extends DefaultClient {

    static Logger logger = Logger.getLogger(ServerClient.class);
    // key - Raft Service URI; value - Raft Device Info
    private HashMap<EndpointReference, RaftDeviceInfo> devicesWeKnow = new HashMap<EndpointReference, RaftDeviceInfo>();
    private Server server;

    public ServerClient(Server s) {
        super();
        server = s;

        // register hello listening
        registerHelloListening();
    }

    public void search() {
        logger.debug(server.getIdString() + "Running SearchingTask...");
        SearchingTask task = new SearchingTask(this, server);
        CoreFramework.getThreadPool().execute(task);
    }

    public void searchRaftDevices() {
        logger.debug(server.getIdString() + "Searching for RAFT devices...");
        SearchParameter search = new SearchParameter();
        search.setDeviceTypes(new QNameSet(Constants.RaftDeviceQName), DPWSCommunicationManager.COMMUNICATION_MANAGER_ID);
        searchDevice(search);
    }

    public int getNumberOfDevices() {
        return devicesWeKnow.size();
    }

    @Override
    public void helloReceived(HelloData helloData) {
        logger.debug(server.getIdString() + " Received HelloData " + helloData);
        EndpointReference recEpr = helloData.getEndpointReference();
        if(!recEpr.equals(server.getEndpointReference()))
        {
            Iterator devicePortTypes = helloData.getDevicePortTypes();
            boolean foundType = false;

            while((!foundType) && (devicePortTypes.hasNext()))
            {
                QName portType = (QName) devicePortTypes.next();
                if(portType.equals(Constants.RaftDeviceQName))
                {
                    foundType = true;
                    insertDevice(helloData);
                }
            }
        }
        else
        {
            logger.debug("Received hello from myself! " + helloData);
        }

    }

    @Override
    public void deviceRunning(DeviceReference deviceRef) {
        logger.debug(server.getIdString() + deviceRef.getEndpointReference() + " - Device running.");

        insertDevice(deviceRef);
    }

    @Override
    public void deviceChanged(DeviceReference deviceRef) {
        logger.debug(server.getIdString() + deviceRef.getEndpointReference() + " - Device changed to metadata version.");

        insertDevice(deviceRef);
    }

    @Override
    public void deviceBuiltUp(DeviceReference deviceRef, Device device) {
        logger.debug(server.getIdString() + deviceRef.getEndpointReference() + " - Device built up");

        insertDevice(deviceRef);
    }

    @Override
    public void deviceBye(DeviceReference deviceRef) {
        logger.debug(server.getIdString() + deviceRef.getEndpointReference() + " - Device has send bye message");

        removeDevice(deviceRef);
    }

    @Override
    public void deviceCommunicationErrorOrReset(DeviceReference deviceRef) {
        logger.debug(server.getIdString() + "An error occured or device has been reset.");

        insertDevice(deviceRef);
    }

    @Override
    public void deviceFound(DeviceReference dr, SearchParameter sp, String string) {
        logger.debug(server.getIdString() + "Device Found: DeviceReference=" + dr + "; SearchParameter=" + sp + "; String=" + string);
        insertDevice(dr);
    }

    public void removeDevice(DeviceReference devRef) {
        logger.debug(server.getIdString() + " Removing Device " + devRef);

        EndpointReference epr = null;

        try {
            Iterator devicePortTypes = devRef.getDevicePortTypes(true);

            boolean foundType = false;

            while((!foundType) && (devicePortTypes.hasNext()))
            {
                QName portType = (QName) devicePortTypes.next();
                if(portType.equals(Constants.RaftDeviceQName))
                {
                    foundType = true;
                    EprInfo eprInfo = (EprInfo) devRef.getDevice().getServiceReference(new URI(Constants.RaftServiceName), SecurityKey.EMPTY_KEY).getEprInfos().next();
                    epr = eprInfo.getEndpointReference();
                    logger.debug(server.getIdString() + " Got Device with Raft Service " + epr);
                }
            }
        } catch (CommunicationException ex) {
            logger.error(ex.getMessage(), ex);
        }

        if ((epr != null) && (!new EndpointReference(server.getRaftService().getAddress()).equals(epr)) && devicesWeKnow.containsKey(epr)) {
            RaftDeviceInfo deadInfo = devicesWeKnow.remove(epr);
            logger.debug(server.getIdString() + " Removed Device with Raft Service " + epr);
            if(deadInfo.getServiceAddress().equalsWsdRfc3986(server.getLeader()))
            {
                logger.debug(server.getIdString() + " Removed Device with Raft Service " + epr + " was the leader. Timeout invoked!");
                server.timeout();
            }
            
        }
    }

    public void removeDevice(RaftDeviceInfo dvcInfo)
    {
        devicesWeKnow.remove(dvcInfo.getDeviceEpr());
    }

    public void insertDevice(DeviceReference dr) {
        logger.debug(server.getIdString() + " insertDevice " + dr);
        EndpointReference epr = dr.getEndpointReference();

        if (!server.getEndpointReference().equals(epr)) {
            insertNewDevice(epr, dr);
        }
    }

    public void insertDevice(HelloData hello) {
        logger.debug(server.getIdString() + " Inserting device with HelloData " + hello);
        Iterator devicePortTypes = hello.getDevicePortTypes();

        boolean foundType = false;

        while((!foundType) && (devicePortTypes.hasNext()))
        {
            QName portType = (QName) devicePortTypes.next();
            if(portType.equals(Constants.RaftDeviceQName))
            {
                foundType = true;
                EndpointReference epr = hello.getEndpointReference();
                DeviceReference devRef = getDeviceReference(hello);
                insertNewDevice(epr, devRef);                
            }
        }
    }

    public void insertNewDevice(EndpointReference epr, DeviceReference deviceReference) {
        logger.debug(server.getIdString() + "Inserting new device with EPR " + epr);
        try {
            // build up device (otherwise device is running but not built
            // up)
            Device newDvc = deviceReference.getDevice();
            // add it to our set so we can look up if we already know it
            RaftDeviceInfo raftDvcInfo = new RaftDeviceInfo(epr, newDvc);

            EndpointReference keyEpr = new EndpointReference(raftDvcInfo.getServiceAddress());
            devicesWeKnow.put(keyEpr, raftDvcInfo);

            if(server.isLeader())
            {
                server.initializeLeaderVariables(keyEpr);
                logger.debug(server.getIdString() + "Initialized Leader Variables for Raft Service " + keyEpr);
            }
        } catch (CommunicationException ex) {
            logger.error(ex.getMessage(), ex);
        }
    }

    public void invokeRequestVoteOp(CandidateTask task, Integer term, URI candidateId, Integer lastLogIndex, Integer lastLogTerm) {
        logger.debug(server.getIdString() + " invokeRequestVoteOp..." + System.currentTimeMillis());
        for (RaftDeviceInfo dvc : devicesWeKnow.values()) {
            VoteRequestingTask voteTask = new VoteRequestingTask(dvc, term, candidateId, lastLogIndex, lastLogTerm, server, task);
            task.addTask(voteTask);
            CoreFramework.getThreadPool().execute(voteTask);
        }
    }

    public void invokeAppendEntriesOp(boolean checkEntries, Integer term, URI leaderId, Integer leaderCommit) {
        int num = devicesWeKnow.size();
        logger.debug(server.getIdString() + " invokeAppendEntriesOp on " + num + " replicas..." + System.currentTimeMillis());
        if (num > 0) {
            try {
                Collection<RaftDeviceInfo> replicas = devicesWeKnow.values();

                Integer lastLogIndex = server.getLastLogIndex();
                for (RaftDeviceInfo dvc : replicas) {
                    ReplicaInvokingTask task = new ReplicaInvokingTask(dvc, checkEntries, lastLogIndex, leaderCommit, server);

                    CoreFramework.getThreadPool().execute(task);
                }
            } catch (AuthorizationException ex) {
                logger.error(ex.getMessage(), ex);
            }
        }

        logger.debug(server.getIdString() + " Finished invokeAppendEntriesOp.");
    }

    public Set<EndpointReference> getKnownDevices() {
        return devicesWeKnow.keySet();
    }

    public void insertServer(EndpointReference epr) {
        if(!epr.equals(server.getEndpointReference()) && !devicesWeKnow.containsKey(epr))
        {
            try {
                Service svc = getServiceReference(epr, DPWSCommunicationManager.COMMUNICATION_MANAGER_ID).getService();
                RaftDeviceInfo newSvcInfo = new RaftDeviceInfo(svc);
                devicesWeKnow.put(epr, newSvcInfo);
            } catch (CommunicationException ex) {
                logger.error("[" + epr + "]:" + ex.getMessage(), ex);
            }

            logger.debug(server.getIdString() + "Inserted Raft Service " + epr);

            if(server.isLeader())
            {
                server.initializeLeaderVariables(epr);
                logger.debug(server.getIdString() + "Initialized Leader Variables for Raft Service " + epr);
            }
        }
        
    }
}
