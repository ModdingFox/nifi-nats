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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"NATS", "Messaging", "Pull", "Ingest", "Ingress", "Topic", "PubSub", "Receive", "Jetstream"})
@CapabilityDescription("Fetches messages from a NATS Messaging Topic")
public class PullSubscribeJetStreamNats extends NatsJetStreamAbstractProcessor {

	protected static final PropertyDescriptor durable = new PropertyDescriptor.Builder().name("durable").displayName("durable").description("Sets the durable consumer name for the subscriber").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor stream = new PropertyDescriptor.Builder().name("stream").displayName("stream").description("pecify the stream to attach to. If not supplied the stream will be looked up by subject").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	super.init(context);
    	
    	//We modify the list of properties to add/remove our jetstream specific bits in
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(this.descriptors);
        descriptors.add(durable);
        descriptors.add(stream);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }
	
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	super.onTrigger(context, session);
    	
    	ArrayList<String> flowFileMessages = new ArrayList<>();
    	
    	if(natsConnectionJetStream != null){
    		PullSubscribeOptions.Builder pullSubscribeOptionsBuilder = PullSubscribeOptions.builder();
    		
    		if(context.getProperty(durable).isSet()) {
    			pullSubscribeOptionsBuilder.durable(context.getProperty(durable).getValue());
        	}
    		
    		if(context.getProperty(stream).isSet()) {
    			pullSubscribeOptionsBuilder.stream(context.getProperty(stream).getValue());
        	}
    		
    		PullSubscribeOptions pullSubscribeOptions = pullSubscribeOptionsBuilder.build();
    		
    		try {
				JetStreamSubscription jetStreamSubscription = natsConnectionJetStream.subscribe(context.getProperty(subject).getValue(), pullSubscribeOptions);
				
				for(Message currentMessage : jetStreamSubscription.fetch(flowFileMessageBatchSize, nextMessageTimeout)) {
					flowFileMessages.add(new String(currentMessage.getData()));
				}
			} catch (IOException e) {
				getLogger().error(this.getClass().getName(), e);
			} catch (JetStreamApiException e) {
				getLogger().error(this.getClass().getName(), e);
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
                getLogger().info("Successfully received {} from NATS(PullJetStream) with {} messages in {} millis", new Object[]{newFlowFile, flowFileMessageBatchSize, millis});
                session.transfer(newFlowFile, REL_SUCCESS);
        	}
    	} else {
    		getLogger().error(this.getClass().getName() + ": Failed to connect to nats(jetstream)");
    	}
    	
    	try {
			natsConnection.close();
		} catch (InterruptedException e) {
			getLogger().error(this.getClass().getName(), e);
		}
    }
}
