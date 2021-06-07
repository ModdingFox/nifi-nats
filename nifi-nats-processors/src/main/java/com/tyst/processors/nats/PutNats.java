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

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.util.StreamDemarcator;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

@Tags({"NATS", "Messaging", "Put", "Publish", "Egress", "Topic", "PubSub", "Send"})
@CapabilityDescription("Publishes messages to a NATS Messaging Topic")
public class PutNats extends NatsAbstractProcessor {
    
	protected static final PropertyDescriptor replyto = new PropertyDescriptor.Builder().name("replyto").displayName("replyto").description("Sends message to a specific reciever on a subject").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	protected static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("FlowFiles that could not be sent to nats").build();
	
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	super.init(context);
    	
    	//We modify the list of properties to add/remove our processor specific bits in
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(this.descriptors);
        descriptors.add(replyto);
        descriptors.remove(queueName);
        descriptors.remove(messageTimeout);
        descriptors.remove(messageBatchSize);
        this.descriptors = Collections.unmodifiableList(descriptors);
        
        //We modify the list of relationships to add our processor specific bits in
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.addAll(this.relationships);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }
	
    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
        
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	super.onTrigger(context, session);
    	
        if (flowFile == null) {
            return;
        }
        
        if(natsConnection != null) {
        	session.read(flowFile, new InputStreamCallback() {
    			@Override
    			public void process(InputStream in) throws IOException {
    		        int messageCount = 0;
    		        
    		        byte [] demarcatorBytes = context.getProperty(demarcator).getValue().getBytes();
    		        
    		        String subjectString = context.getProperty(subject).getValue();
    		        
    		        boolean isReplyTo = context.getProperty(replyto).isSet();
    		        String replyToString = ((context.getProperty(replyto).isSet())?(context.getProperty(replyto).getValue()):(null));
    		        
    				
    				StreamDemarcator streamDemarcator = new StreamDemarcator(in, demarcatorBytes, Integer.MAX_VALUE);
    				
    				boolean endOfCurrentData = false;
    				byte [] currentData = null;
    				
    				do {
    					currentData = streamDemarcator.nextToken();
    					
    					if(currentData != null) {
    						messageCount++;
    						if(isReplyTo) {
    							natsConnection.publish(subjectString, replyToString, currentData);
    						} else {
    							natsConnection.publish(subjectString, currentData);
    						}
    					} else {
    						endOfCurrentData = true;
    					}
    				} while(!endOfCurrentData);
    				
    				streamDemarcator.close();
    				
    				//Would be nice to detect success or fails
    		        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    		        session.getProvenanceReporter().receive(flowFile, natsConnection.getServers().stream().collect(Collectors.joining(",")) + " " + context.getProperty(subject).getValue(), "Sent " + messageCount + " NATS messages", millis);
    		        getLogger().info("Successfully sent {} to NATS with {} messages in {} millis", new Object[]{flowFile, messageCount, millis});
    			}
    		});
            
            session.transfer(flowFile, REL_SUCCESS);
        } else {
        	session.transfer(flowFile, REL_FAILURE);
        	getLogger().error(this.getClass().getName() + ": Failed to connect to nats");
        }
        
    	try {
			natsConnection.close();
		} catch (InterruptedException e) {
			getLogger().error(this.getClass().getName(), e);
		}
    }
}
