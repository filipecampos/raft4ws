/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package pt.uminho.di.raft.entities.workers;

import java.util.List;
import org.apache.log4j.Logger;
import org.ws4d.java.authorization.AuthorizationException;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.types.EndpointReference;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.service.RaftDeviceInfo;
import pt.uminho.di.raft.service.log.LogEntry;

/**
 *
 * @author fcampos
 */
public class ReplicaInvokingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(ReplicaInvokingTask.class);

    private RaftDeviceInfo dvcInfo;
    private boolean checkEntries;
    private Integer lastLogIndex;
    private Integer commitIndex;
    private EndpointReference svcEpr;
    private ParameterValue request;
    private List<LogEntry> entries = null;

    public ReplicaInvokingTask(RaftDeviceInfo info, boolean ce, Integer lastIndex, Integer commit, Server srv)
    {
        super(srv);
        dvcInfo = info;
        checkEntries = ce;
        lastLogIndex = lastIndex;
        commitIndex = commit;
        svcEpr = new EndpointReference(dvcInfo.getServiceAddress());
    }

    @Override
    public void run()
    {
        if(isRunning())
        {
            try
            {
                Integer nextIndex = getServer().getNextIndex(svcEpr);
             
                if (nextIndex == null) {
                    logger.error(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - nextIndex is null!! Initializing leader variables...");
                    getServer().initializeLeaderVariables();
                    nextIndex = 0;
                }

                Integer prevLogIndex = nextIndex - 1;
                if (checkEntries) {
                    entries = getServer().getEntriesFromTo(nextIndex, lastLogIndex+1);
                    logger.debug(getServer().getIdString() + "Got these entries " + entries);
                }

                Integer prevLogTerm = -1;
                if(prevLogIndex > 0)
                {
                    LogEntry prevEntry = getServer().getLogEntry(prevLogIndex);
                    if(prevEntry != null)
                        prevLogTerm = prevEntry.getTerm();
                }

                request = getServer().getRaftService().getAppendEntriesOp().getRequestPV(getServer().getCurrentTerm(), getServer().getLeader(), prevLogIndex, prevLogTerm, entries, commitIndex);
                logger.debug(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - Invoking Request PV: " + request);
                process();
            } catch(InvocationException ex)
            {
                logger.error(ex.getMessage(), ex);
            } catch(CommunicationException ex)
            {
                logger.error(ex.getMessage(), ex);
                logger.error("Message sent was: " + request);
                if(ex.getCause().getClass().equals(java.lang.NullPointerException.class))
                {
                    logger.warn(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - Removing service " + dvcInfo.getServiceAddress());
                    getServer().getClient().removeDevice(dvcInfo);
                }

//                try {
//                    //                getServer().decrementNextIndex(svcEpr);
//                    //                CoreFramework.getThreadPool().execute(new ReplicaInvokingTask(dvcInfo, checkEntries, lastLogIndex, commitIndex, getServer()));
//                    process();
//                } catch (InvocationException ex1) {
//                    logger.error(ex1.getMessage(), ex1);
//                } catch (CommunicationException ex1) {
//                    logger.error(ex1.getMessage(), ex1);
//                    logger.error("Message sent was: " + request);
//                }
                
            } catch(AuthorizationException ex)
            {
                logger.error(ex.getMessage(), ex);
            }
        }
    }

    private void process() throws InvocationException, CommunicationException
    {
        ParameterValue response = dvcInfo.getAppendEntriesOp().invoke(request, CredentialInfo.EMPTY_CREDENTIAL_INFO);

                logger.debug(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - Received Response PV: " + response);
                String responseTermStr = ParameterValueManagement.getString(response, Constants.TermElementName);
                Integer responseTerm = Integer.parseInt(responseTermStr);

                if (getServer().updateTerm(responseTerm)) {
                    logger.debug(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - No more processing as server is not the leader anymore!");
                } else {
                    String responseSuccessStr = ParameterValueManagement.getString(response, Constants.SuccessElementName);
                    Boolean responseSuccess = Boolean.parseBoolean(responseSuccessStr);

                    if (responseSuccess) {
                        getServer().receivedReplicaResponse(svcEpr, lastLogIndex, commitIndex, entries);
                    }
                    else
                    {
                        logger.debug(getServer().getIdString() + "ReplicaInvokingTask[" + svcEpr + "] - Processing unsuccessful response...");
                        getServer().decrementNextIndex(svcEpr);
                    }
                }
    }

}
