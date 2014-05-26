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

package pt.uminho.di.raft.service;

import org.apache.log4j.Logger;
import org.ws4d.java.service.DefaultService;
import org.ws4d.java.types.EprInfo;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;
import pt.uminho.di.raft.entities.Server;
import pt.uminho.di.raft.service.operations.AppendEntriesOperation;
import pt.uminho.di.raft.service.operations.InsertCommandOperation;
import pt.uminho.di.raft.service.operations.ReadOperation;
import pt.uminho.di.raft.service.operations.RequestVoteOperation;

public class RaftService extends DefaultService {

    static Logger logger = Logger.getLogger(RaftService.class);

    protected Server server;

    private RequestVoteOperation requestVoteOp;
    private AppendEntriesOperation appendEntriesOp;
    protected InsertCommandOperation insertCommandOp;
    protected ReadOperation readOp;
    
    public RaftService(Server s)
    {
        server = s;

        setServiceId(Constants.RaftServiceId);

        server.setRaftService(this);

        initializeOperations();
    }

    public void initializeOperations()
    {
        requestVoteOp = new RequestVoteOperation(server);
        addOperation(requestVoteOp);
        appendEntriesOp = new AppendEntriesOperation(server);
        addOperation(appendEntriesOp);
        insertCommandOp = new InsertCommandOperation(server);
        addOperation(insertCommandOp);
        readOp = new ReadOperation(server);
        addOperation(readOp);
    }

    public AppendEntriesOperation getAppendEntriesOp()
    {
        return appendEntriesOp;
    }

    public RequestVoteOperation getRequestVoteOp()
    {
        return requestVoteOp;
    }

    public InsertCommandOperation getInsertCommandOp()
    {
        return insertCommandOp;
    }

    public ReadOperation getReadOp() {
        return readOp;
    }

    public URI getAddress()
    {
        EprInfo eprInfo = (EprInfo) getEprInfos().next();
        return eprInfo.getXAddress();
    }
    
}
