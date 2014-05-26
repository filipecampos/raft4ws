/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pt.uminho.di.raft.entities.workers;

import org.apache.log4j.Logger;
import org.ws4d.java.authorization.AuthorizationException;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.states.CandidateTask;
import pt.uminho.di.raft.service.RaftDeviceInfo;

/**
 *
 * @author fcampos
 */
public class VoteRequestingTask extends ServerWorkingTask {

    static Logger logger = Logger.getLogger(VoteRequestingTask.class);
    private RaftDeviceInfo dvcInfo;
    private URI candidateId;
    private Integer term;
    private Integer lastLogIndex;
    private Integer lastLogTerm;
    private CandidateTask task;

    public VoteRequestingTask(RaftDeviceInfo info, Integer t, URI cId, Integer lastIndex, Integer lastTerm, Server srv, CandidateTask cand) {
        super(srv);
        dvcInfo = info;
        candidateId = cId;
        term = t;
        lastLogIndex = lastIndex;
        lastLogTerm = lastTerm;
        task = cand;
    }

    @Override
    public void run() {
        if (isRunning()) {
            EndpointReference targetSvcEpr = new EndpointReference(dvcInfo.getServiceAddress());
            logger.debug(getServer().getIdString() + "Invoking RequestVote on server " + targetSvcEpr + "...");

            ParameterValue request = getServer().getRaftService().getRequestVoteOp().getRequestPV(term, candidateId, lastLogIndex, lastLogTerm);

            try {
                ParameterValue response = dvcInfo.getRequestVoteOp().invoke(request, CredentialInfo.EMPTY_CREDENTIAL_INFO);

                logger.debug(getServer().getIdString() + "Received Response PV: " + response + " at " + System.currentTimeMillis());

                if(task.electedLeader())    // vote processing not required
                {
                    logger.debug(getServer().getIdString() + "Candidate has already been elected the leader!");
                }
                else
                {
                    String responseTermStr = ParameterValueManagement.getString(response, "term");
                    Integer responseTerm = Integer.parseInt(responseTermStr);

                    String responseVoteGrantedStr = ParameterValueManagement.getString(response, "voteGranted");
                    Boolean responseVoteGranted = Boolean.parseBoolean(responseVoteGrantedStr);

                    // replica's currentTerm, for candidate to update itself
                    if (getServer().updateTerm(responseTerm)) {
                        // Candidate found a server with higher term. Turning into Follower
                        logger.debug(getServer().getIdString() + "Candidate found server " + targetSvcEpr + " with higher term. Finishing election...");
                    } else {
                        // process vote
                        if (responseVoteGranted) {
                            // add replica vote and check if majority has been reached
                            boolean finishedElection = task.addVote(targetSvcEpr);
                            logger.debug(getServer().getIdString() + "Candidate got vote from server " + targetSvcEpr + ". Majority reached? " + finishedElection);
                        }
                    }
                }
            } catch (InvocationException ex) {
                logger.error(ex.getMessage(), ex);
            } catch (CommunicationException ex) {
                logger.error(ex.getMessage(), ex);
                // remove server from knownDevices
                this.getServer().getClient().removeDevice(dvcInfo);
            } catch (AuthorizationException ex) {
                logger.error(ex.getMessage(), ex);
            }
        }
    }
}
