package com.tyst.processors.nats;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;

public class NatsJetStreamAbstractProcessor extends NatsAbstractProcessor {
	
	protected static final PropertyDescriptor prefix = new PropertyDescriptor.Builder().name("prefix").displayName("prefix").description("Sets the prefix for JetStream subjects").required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	protected static final PropertyDescriptor publishNoAck = new PropertyDescriptor.Builder().name("publishNoAck").displayName("publishNoAck").description("Sets whether the streams in use by contexts created with these options are no-ack streams").required(true).defaultValue("false").allowableValues("false", "true").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    protected static final PropertyDescriptor requestTimeout = new PropertyDescriptor.Builder().name("requestTimeout").displayName("requestTimeout").description("Sets the request timeout for JetStream API calls(millis)").required(true).defaultValue("5000").addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR).build();
	
	
	protected JetStream natsConnectionJetStream = null;
	
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	super.init(context);
    	
    	//We modify the list of properties to add/remove our jetstream specific bits in
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(this.descriptors);
        descriptors.add(prefix);
        descriptors.add(publishNoAck);
        descriptors.add(requestTimeout);
        descriptors.remove(queueName);
        this.descriptors = Collections.unmodifiableList(descriptors);
    }
	
    protected JetStream initJetStreamConnection(ProcessContext context) {
    	JetStream result = null;
    	
    	JetStreamOptions.Builder jetStreamOptionsBuilder = JetStreamOptions.builder();
    	
    	if(context.getProperty(prefix).isSet()) {
    	    jetStreamOptionsBuilder.prefix(context.getProperty(prefix).getValue());
    	}
    	
    	jetStreamOptionsBuilder.publishNoAck(context.getProperty(prefix).getValue().equals("true"));
    	jetStreamOptionsBuilder.requestTimeout(Duration.ofMillis(parseLongWithDefault(context.getProperty(requestTimeout).getValue(), requestTimeout.getDefaultValue())));
    	
    	JetStreamOptions jetStreamOptions = jetStreamOptionsBuilder.build();
    	
    	try {
			result = natsConnection.jetStream(jetStreamOptions);
		} catch (IOException e) {
			getLogger().error(this.getClass().getName(), e);
		}
    	
    	return result;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	super.onTrigger(context, session);
    	
    	if(natsConnection != null) {
    		natsConnectionJetStream = initJetStreamConnection(context);
    	} else {
    		getLogger().error(this.getClass().getName() + ": Failed to connect to nats");
    	}
    }
}
