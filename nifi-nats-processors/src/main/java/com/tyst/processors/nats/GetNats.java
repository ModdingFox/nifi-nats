/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tyst.processors.nats;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import io.nats.client.Message;
import io.nats.client.Subscription;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"NATS", "Messaging", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Receive"})
@CapabilityDescription("Fetches messages from a NATS Messaging Topic")
public class GetNats extends NatsAbstractProcessor {
        
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	super.onTrigger(context, session);
    	
    	ArrayList<String> flowFileMessages = new ArrayList<>();
    	
    	if(natsConnection != null) {
    	    Subscription subscription = null;
    	    
    	    if(context.getProperty(subject).isSet() && context.getProperty(queueName).isSet()) {
    	        subscription = natsConnection.subscribe(context.getProperty(subject).getValue(), context.getProperty(queueName).getValue());
    	    } else {
    	        subscription = natsConnection.subscribe(context.getProperty(subject).getValue());
    	    }
    	    
    	    if(subscription != null) {
    	        try {
                    while(flowFileMessages.size() < flowFileMessageBatchSize) {
                        Message currentMessage = subscription.nextMessage(nextMessageTimeout);
                        flowFileMessages.add(new String(currentMessage.getData()));
                    }
                    subscription.unsubscribe();
				} catch (IllegalStateException e) {
				    getLogger().error(this.getClass().getName(), e);
				} catch (InterruptedException e) {
					getLogger().error(this.getClass().getName(), e);
				}
    	    } else {
    	        getLogger().error(this.getClass().getName() + ": Failed to create nats subscription");
    	    }
    	} else {
    	    getLogger().error(this.getClass().getName() + ": Failed to connect to nats");
    	}
    	
    	if(flowFileMessages.size() > 0) {
            FlowFile newFlowFile = session.create();
            final Charset charset = Charset.forName("UTF-8");
            final byte[] flowFileContentBytes = flowFileMessages.stream().collect(Collectors.joining(context.getProperty(demarcator).getValue())).getBytes(charset);
            
            newFlowFile = session.append(newFlowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(flowFileContentBytes);
                }
            });
            
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            session.getProvenanceReporter().receive(newFlowFile, natsConnection.getServers().stream().collect(Collectors.joining(",")) + " " + context.getProperty(subject).getValue(), "Received " + flowFileMessageBatchSize + " NATS messages", millis);
            getLogger().info("Successfully received {} from NATS with {} messages in {} millis", new Object[]{newFlowFile, flowFileMessageBatchSize, millis});
            session.transfer(newFlowFile, REL_SUCCESS);
    	}
    	
    	try {
			natsConnection.close();
		} catch (InterruptedException e) {
			getLogger().error(this.getClass().getName(), e);
		}
    }
}
