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

package pt.uminho.di.raft.service.operations;

import org.apache.log4j.Logger;
import org.ws4d.java.CoreFramework;
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.schema.ComplexType;
import org.ws4d.java.schema.Element;
import org.ws4d.java.schema.SchemaUtil;
import org.ws4d.java.security.CredentialInfo;
import org.ws4d.java.service.InvocationException;
import org.ws4d.java.service.parameter.ParameterValue;
import org.ws4d.java.service.parameter.ParameterValueManagement;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.entities.workers.InsertingTask;

/**
 *
 * Invoked by candidates to gather votes (§5.2).
 *
 * Arguments:
 * -term - candidate's term
 * -candidateId - candidate requesting vote
 * -lastLogIndex - index of candidate's last log entry (§5.4)
 * -lastLogTerm - term of candidate's last log entry (§5.4)
 * 
 * Results:
 * -term - currentTerm, for candidate to update itself
 * -voteGranted - true means candidate received vote
 *
 * Implementation:
 * 1. Reply false if term < currentTerm (§5.1)
 * 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
 */
public class RequestVoteOperation extends RaftOperation {

    static final Logger logger = Logger.getLogger(RequestVoteOperation.class);

    public RequestVoteOperation(Server s)
    {
        super(s, Constants.RequestVoteOperationName, Constants.RaftServiceQName);
    }

    @Override
    public void initInput()
    {
        Element termElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        Element candidateIdElem = new Element(Constants.CandidateIdElementQName, SchemaUtil.TYPE_ANYURI);
        Element lastLogIndexElem = new Element(Constants.LastLogIndexElementQName, SchemaUtil.TYPE_INTEGER);
        Element lastLogTermElem = new Element(Constants.LastLogTermElementQName, SchemaUtil.TYPE_INTEGER);

        ComplexType requestType = new ComplexType(Constants.RequestVoteRequestTypeQName, ComplexType.CONTAINER_SEQUENCE);
        requestType.addElement(termElem);
        requestType.addElement(candidateIdElem);
        requestType.addElement(lastLogIndexElem);
        requestType.addElement(lastLogTermElem);

        Element request = new Element(Constants.RequestVoteRequestElementQName, requestType);
        setInput(request);
    }

    @Override
    public void initOutput()
    {
        Element termElem = new Element(Constants.TermElementQName, SchemaUtil.TYPE_INTEGER);
        Element voteGrantedElem = new Element(Constants.VoteGrantedElementQName, SchemaUtil.TYPE_BOOLEAN);

        ComplexType responseType = new ComplexType(Constants.RequestVoteResponseTypeQName, ComplexType.CONTAINER_SEQUENCE);
        responseType.addElement(termElem);
        responseType.addElement(voteGrantedElem);

        Element response = new Element(Constants.RequestVoteResponseElementQName, responseType);
        setOutput(response);
    }

    @Override
    protected ParameterValue invokeImpl(ParameterValue pv, CredentialInfo ci) throws InvocationException, CommunicationException
    {
        ParameterValue response = createOutputValue();
        boolean responseVoteGranted = false;

        Integer currentTerm = server.getCurrentTerm();

        try
        {
            String termStr = ParameterValueManagement.getString(pv, Constants.TermElementName);
            Integer term = Integer.parseInt(termStr);
            logger.debug(server.getIdString() + "Received RequestVote request. Current Term=" + currentTerm + " ; term=" + term);
            if(server.updateTerm(term))
            {
                // retrieve updated term
                currentTerm = server.getCurrentTerm();
            }

            // 1. Reply false if term < currentTerm (§5.1)
            if(term >= currentTerm)
            {
                // 2. If votedFor is null or candidateId, and candidate's log is
                // at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
                URI votedFor = server.getVotedFor();
                String candidateIdStr = ParameterValueManagement.getString(pv, Constants.CandidateIdElementName);
                URI candidateIdUri = new URI(candidateIdStr);
                InsertingTask insertingTask = new InsertingTask(server.getClient(), server, new EndpointReference(candidateIdUri));
                CoreFramework.getThreadPool().execute(insertingTask);

                logger.debug(server.getIdString() + "votedFor=" + votedFor + " candidateId=" + candidateIdUri);
                if((votedFor == null) || (votedFor.equalsWsdRfc3986(Constants.emptyURI)) || (votedFor.equalsWsdRfc3986(candidateIdUri)))
                {
                    String lastLogIndexStr = ParameterValueManagement.getString(pv, Constants.LastLogIndexElementName);
                    Integer lastLogIndex = Integer.parseInt(lastLogIndexStr);
                    Integer commitIndex = server.getLastLogIndex();

                    String lastLogTermStr = ParameterValueManagement.getString(pv, Constants.LastLogTermElementName);
                    Integer lastLogTerm = Integer.parseInt(lastLogTermStr);
                    Integer serverLastLogTerm = server.getLastLogTerm();

                    logger.debug(server.getIdString() + "lastLogIndex=" + lastLogIndexStr + "; serverIndex=" + commitIndex
                        + "; lastLogTerm=" + lastLogTermStr + "; serverTerm=" + serverLastLogTerm);

                    if((lastLogIndex >= commitIndex) && (lastLogTerm >= serverLastLogTerm))
                    {
                        // should be follower to issue requested vote
                        logger.debug(server.getIdString() + "Issuing vote to " + candidateIdStr);
                        server.contactedBy(candidateIdUri, false, true);
                        responseVoteGranted = true;
                        server.setVotedFor(candidateIdUri);
                    }
                }
            }
        }
        catch(Exception ex)
        {
            logger.error(ex.getMessage(), ex);
        }

        ParameterValueManagement.setString(response, Constants.TermElementName, currentTerm.toString());
        ParameterValueManagement.setString(response, Constants.VoteGrantedElementName, "" + responseVoteGranted);

        logger.debug(server.getIdString() + "Responding with term=" + currentTerm.toString() + "; voteGranted=" + responseVoteGranted);

        return response;
    }

    public ParameterValue getRequestPV(Integer term, URI candidateId, Integer lastLogIndex, Integer lastLogTerm)
    {
        ParameterValue reply = createInputValue();

        ParameterValueManagement.setString(reply, Constants.TermElementName, term.toString());
        ParameterValueManagement.setString(reply, Constants.CandidateIdElementName, candidateId.toString());
        ParameterValueManagement.setString(reply, Constants.LastLogIndexElementName, lastLogIndex.toString());
        ParameterValueManagement.setString(reply, Constants.LastLogTermElementName, lastLogTerm.toString());
        logger.debug(server.getIdString() + "Created Request PV: " + reply);
        
        return reply;
    }
}
