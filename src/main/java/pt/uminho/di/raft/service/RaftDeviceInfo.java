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
import org.ws4d.java.communication.CommunicationException;
import org.ws4d.java.security.SecurityKey;
import org.ws4d.java.service.Device;
import org.ws4d.java.service.Operation;
import org.ws4d.java.service.Service;
import org.ws4d.java.structures.Iterator;
import org.ws4d.java.types.EndpointReference;
import org.ws4d.java.types.EprInfo;
import org.ws4d.java.types.URI;
import pt.uminho.di.raft.Constants;

public class RaftDeviceInfo {

    static Logger logger = Logger.getLogger(RaftDeviceInfo.class);
    EndpointReference dvcEpr;
    URI serviceEpr;
    Operation requestVoteOp;
    Operation appendEntriesOp;

    public RaftDeviceInfo(EndpointReference endpoint, Device newDvc) {
        dvcEpr = endpoint;
        try {
            URI raftSvcIdUri = new URI(Constants.RaftServiceName);
            Service svc = newDvc.getServiceReference(raftSvcIdUri, SecurityKey.EMPTY_KEY).getService();

            if (svc != null) {
                init(svc);
            } else {
                logger.error("Trying to get Raft Service but got null!");

            }
        } catch (CommunicationException ex) {
            logger.error("[" + dvcEpr + "]:" + ex.getMessage(), ex);
        }
    }

    public RaftDeviceInfo(Service svc) {
        if (svc != null) {
            dvcEpr = svc.getParentDeviceReference(SecurityKey.EMPTY_KEY).getEndpointReference();
            init(svc);
        } else {
            logger.error("Trying to get Raft Service but got null!");
        }
    }

    private void init(Service svc)
    {
        Iterator infos = svc.getEprInfos();
        if (infos.hasNext()) {
            EprInfo eprInfo = (EprInfo) infos.next();
            logger.debug("Info=" + eprInfo + "; serviceEpr=" + eprInfo.getXAddress());
            serviceEpr = eprInfo.getXAddress();
        }

        // requestVoteOp
        requestVoteOp = svc.getOperation(Constants.RaftServiceQName, Constants.RequestVoteOperationName, null, null);

        // appendEntriesOp
        appendEntriesOp = svc.getOperation(Constants.RaftServiceQName, Constants.AppendEntriesOperationName, null, null);
    }

    public Operation getAppendEntriesOp() {
        return appendEntriesOp;
    }

    public EndpointReference getDeviceEpr() {
        return dvcEpr;
    }

    public Operation getRequestVoteOp() {
        return requestVoteOp;
    }

    public URI getServiceAddress() {
        return serviceEpr;
    }
}
