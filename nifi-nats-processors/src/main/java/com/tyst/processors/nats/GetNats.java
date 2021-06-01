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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.Subscription;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"NATS", "Messaging", "Get", "Ingest", "Ingress", "Topic", "PubSub", "Receive"})
@CapabilityDescription("Fetches messages from a NATS Messaging Topic")
public class GetNats extends AbstractProcessor {

    public static final PropertyDescriptor bufferSize = new PropertyDescriptor.Builder().name("bufferSize").displayName("bufferSize").description("Sets the initial size for buffers in the connection(bytes)").required(true).defaultValue("8000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor connectionName = new PropertyDescriptor.Builder().name("connectionName").displayName("connectionName").description("Set the connection's optional Name").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor connectionTimeout = new PropertyDescriptor.Builder().name("connectionTimeout").displayName("connectionTimeout").description("Set the timeout for connection attempts(millis)").required(true).defaultValue("5000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor maxControlLine = new PropertyDescriptor.Builder().name("maxControlLine").displayName("maxControlLine").description("Set the maximum length of a control line sent by this connection(bytes)").required(true).defaultValue("8000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor maxPingsOut = new PropertyDescriptor.Builder().name("maxPingsOut").displayName("maxPingsOut").description("Set the maximum number of pings the client can have in flight").required(true).defaultValue("8").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor maxReconnects = new PropertyDescriptor.Builder().name("maxReconnects").displayName("maxReconnects").description("Set the maximum number of reconnect attempts").required(true).defaultValue("3").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor pingInterval = new PropertyDescriptor.Builder().name("pingInterval").displayName("pingInterval").description("Set the interval between attempts to pings the server").required(true).defaultValue("1000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor reconnectBufferSize = new PropertyDescriptor.Builder().name("reconnectBufferSize").displayName("reconnectBufferSize").description("Set the maximum number of bytes to buffer in the client when trying to reconnect(bytes)").required(true).defaultValue("8000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor reconnectWait = new PropertyDescriptor.Builder().name("reconnectWait").displayName("reconnectWait").description("Set the time to wait between reconnect attempts to the same server(millis)").required(true).defaultValue("1000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor requestCleanupInterval = new PropertyDescriptor.Builder().name("requestCleanupInterval").displayName("requestCleanupInterval").description("Set the interval between cleaning passes on outstanding request futures that are cancelled or timeout in the application code(millis)").required(true).defaultValue("1000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor token = new PropertyDescriptor.Builder().name("token").displayName("token").description("Set the token for token-based authentication").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor userName = new PropertyDescriptor.Builder().name("userName").displayName("userName").description("Set the username for basic authentication").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor password = new PropertyDescriptor.Builder().name("password").displayName("password").description("Set the password for basic authentication.").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor protocol = new PropertyDescriptor.Builder().name("protocol").displayName("protocol").description("Protocol to use when connecting to nats(nats/tls). TLS not currently supported.").required(true).defaultValue("nats").allowableValues("nats").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor urls = new PropertyDescriptor.Builder().name("urls").displayName("urls").description("Add an array of servers to the list of known servers. Format should be \"host:port,host:port,host:port\"").required(true).defaultValue("0.0.0.0:4222").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor subject = new PropertyDescriptor.Builder().name("subject").displayName("subject").description("The subject to subscribe to").required(true).defaultValue("*").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor queueName = new PropertyDescriptor.Builder().name("queueName").displayName("queueName").description("The queue group to join").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor messageTimeout = new PropertyDescriptor.Builder().name("messageTimeout").displayName("messageTimeout").description("The time to wait for the next message(millis)").required(true).defaultValue("1000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor messageBatchSize = new PropertyDescriptor.Builder().name("messageBatchSize").displayName("messageBatchSize").description("The number of messages to output per flowfile").required(true).defaultValue("1").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
    public static final PropertyDescriptor demarcator = new PropertyDescriptor.Builder().name("demarcator").displayName("demarcator").description("The string that will be used for demarcating messages when flowfiles have mulitple messages").required(true).defaultValue("\n").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("FlowFiles received from nats").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(bufferSize);
        descriptors.add(connectionName);
        descriptors.add(connectionTimeout);
        descriptors.add(maxControlLine);
        descriptors.add(maxPingsOut);
        descriptors.add(maxReconnects);
        descriptors.add(pingInterval);
        descriptors.add(reconnectBufferSize);
        descriptors.add(reconnectWait);
        descriptors.add(requestCleanupInterval);
        descriptors.add(token);
        descriptors.add(userName);
        descriptors.add(password);
        descriptors.add(protocol);
        descriptors.add(urls);
        descriptors.add(subject);
        descriptors.add(queueName);
        descriptors.add(messageTimeout);
        descriptors.add(messageBatchSize);
        descriptors.add(demarcator);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }
    
    private int parseIntWithDefault(String stringIntIn, String defaultValue) {
        int result = -1;
        
        try{
           result = Integer.parseInt(stringIntIn); 
        } catch (NumberFormatException e) {
            getLogger().error(this.getClass().getName(), e);
            try{
                result = Integer.parseInt(defaultValue);
            } catch (NumberFormatException e1) {
                getLogger().error(this.getClass().getName(), e1);
                result = -1;
            }
        }
        
        return result;
    }
    
        private long parseLongWithDefault(String stringLongIn, String defaultValue) {
        long result = -1;
        
        try{
           result = Long.parseLong(stringLongIn); 
        } catch (NumberFormatException e) {
            getLogger().error(this.getClass().getName(), e);
            try{
                result = Long.parseLong(defaultValue);
            } catch (NumberFormatException e1) {
                getLogger().error(this.getClass().getName(), e1);
                result = -1;
            }
        }
        
        return result;
    }
    
    private Connection initConnection(ProcessContext context) {
        Connection result = null;
        
        Options.Builder optionsBuilder = new Options.Builder();
        
        optionsBuilder.connectionListener(new ConnectionListener() {

			@Override
			public void connectionEvent(Connection arg0, Events arg1) {
				getLogger().info(this.getClass().getName() + ": connectionEvent on " + arg0.getConnectedUrl() + " " + arg1.name());
			}
        });
        
        optionsBuilder.errorListener(new ErrorListener() {
			
			@Override
			public void slowConsumerDetected(Connection arg0, Consumer arg1) {
				getLogger().warn(this.getClass().getName() + ": slowConsumerDetected on " + arg0.getConnectedUrl() + "\n" + 
				        "PendingMessages: " + arg1.getPendingMessageCount() + "/" + arg1.getPendingMessageLimit() + "\n" +
				        "PendingBytes: " + arg1.getPendingByteCount()  + "/" + arg1.getPendingByteLimit() + "\n" +
				        "Delivered: " + arg1.getDeliveredCount() + "\n" +
				        "Dropped: " + arg1.getDroppedCount()
				    );
			}
			
			@Override
			public void exceptionOccurred(Connection arg0, Exception arg1) {
			    getLogger().error(this.getClass().getName() + ": exceptionOccurred on " + arg0.getConnectedUrl(), arg1);				
			}
			
			@Override
			public void errorOccurred(Connection arg0, String arg1) {
				getLogger().error(this.getClass().getName() + ": errorOccurred on " + arg0.getConnectedUrl() + " " + arg1);	
			}
		});
		
        optionsBuilder.bufferSize(parseIntWithDefault(context.getProperty(bufferSize).getValue(), bufferSize.getDefaultValue()));
        
        if(context.getProperty(bufferSize).isSet()) {
            optionsBuilder.connectionName(context.getProperty(connectionName).getValue());
        }
        
        optionsBuilder.connectionTimeout(Duration.ofMillis(parseLongWithDefault(context.getProperty(connectionTimeout).getValue(), connectionTimeout.getDefaultValue())));
        //.dataPortType(dataPortClassName)
        optionsBuilder.maxControlLine(parseIntWithDefault(context.getProperty(maxControlLine).getValue(), maxControlLine.getDefaultValue()));
        
        optionsBuilder.maxPingsOut(parseIntWithDefault(context.getProperty(maxPingsOut).getValue(), maxPingsOut.getDefaultValue()));
        optionsBuilder.maxReconnects(parseIntWithDefault(context.getProperty(maxReconnects).getValue(), maxReconnects.getDefaultValue()));    
        //.noRandomize(noRandomize)
        //.noReconnect(noReconnect)
        //.oldRequestStyle(oldRequestStyle)
        //.opentls(opentls)
        //.pedantic(pedantic)
        optionsBuilder.pingInterval(Duration.ofMillis(parseLongWithDefault(context.getProperty(pingInterval).getValue(), pingInterval.getDefaultValue())));
        optionsBuilder.reconnectBufferSize(parseIntWithDefault(context.getProperty(reconnectBufferSize).getValue(), reconnectBufferSize.getDefaultValue()));
        optionsBuilder.reconnectWait(Duration.ofMillis(parseLongWithDefault(context.getProperty(reconnectWait).getValue(), reconnectWait.getDefaultValue())));
        optionsBuilder.requestCleanupInterval(Duration.ofMillis(parseLongWithDefault(context.getProperty(requestCleanupInterval).getValue(), requestCleanupInterval.getDefaultValue()))); 
        //.secure(secure)
        //.sslContext(sslContext)
        
        if(context.getProperty(token).isSet()) {
            optionsBuilder.connectionName(context.getProperty(token).getValue());
        } else if(context.getProperty(userName).isSet() && context.getProperty(password).isSet()) {
            optionsBuilder.userInfo(context.getProperty(userName).getValue(), context.getProperty(password).getValue());
        }
        
        String [] splitUrls = context.getProperty(urls).getValue().split(",");
        for(int i = 0; i < splitUrls.length; i++) {
            splitUrls[i] = context.getProperty(protocol).getValue() + "://" + splitUrls[i];
        }
        optionsBuilder.servers(splitUrls);
        
        Options options = optionsBuilder.build();
        
        try {
            result = Nats.connect(options);
        } catch (InterruptedException e) {
            getLogger().error(this.getClass().getName(), e);
        } catch (IOException e) {
            getLogger().error(this.getClass().getName(), e);
		}
        
        return result;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final long start = System.nanoTime();
    
    	Connection natsConnection = initConnection(context);
    	
    	Duration nextMessageTimeout = Duration.ofMillis(parseLongWithDefault(context.getProperty(messageTimeout).getValue(), messageTimeout.getDefaultValue()));
    	int flowFileMessageBatchSize = parseIntWithDefault(context.getProperty(messageBatchSize).getValue(), messageBatchSize.getDefaultValue());
    	ArrayList<String> flowFileMessages = new ArrayList<>();
    	
    	if(natsConnection != null){
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
            FlowFile flowFile = session.create();
            final Charset charset = Charset.forName("UTF-8");
            final byte[] flowFileContentBytes = flowFileMessages.stream().collect(Collectors.joining(context.getProperty(demarcator).getValue())).getBytes(charset);
            
            flowFile = session.append(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(flowFileContentBytes);
                }
            });
            
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            session.getProvenanceReporter().receive(flowFile, natsConnection.getServers().stream().collect(Collectors.joining(",")) + " " + context.getProperty(subject).getValue(), "Received " + flowFileMessageBatchSize + " NATS messages", millis);
            getLogger().info("Successfully received {} from NATS with {} messages in {} millis", new Object[]{flowFile, flowFileMessageBatchSize, millis});
            session.transfer(flowFile, REL_SUCCESS);
    	}
    	
    	try {
			natsConnection.close();
		} catch (InterruptedException e) {
			getLogger().error(this.getClass().getName(), e);
		}
    }
}
