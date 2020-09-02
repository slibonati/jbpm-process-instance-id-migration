package com.redhat.piidmigration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.drools.core.common.InternalWorkingMemory;
import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.StatefulKnowledgeSessionImpl;
import org.drools.core.marshalling.impl.InputMarshaller;
import org.drools.core.marshalling.impl.MarshallerReaderContext;
import org.drools.core.marshalling.impl.MarshallerWriteContext;
import org.drools.core.marshalling.impl.PersisterHelper;
import org.drools.core.marshalling.impl.ProcessMarshallerWriteContext;
import org.drools.core.marshalling.impl.ProtobufInputMarshaller;
import org.drools.core.marshalling.impl.ProtobufMarshaller;
import org.drools.core.marshalling.impl.ProtobufOutputMarshaller;
import org.drools.core.process.instance.WorkItem;
import org.jbpm.marshalling.impl.JBPMMessages;
import org.jbpm.marshalling.impl.ProcessInstanceMarshaller;
import org.jbpm.marshalling.impl.ProcessMarshallerRegistry;
import org.jbpm.marshalling.impl.ProtobufRuleFlowProcessInstanceMarshaller;
import org.jbpm.process.instance.impl.ProcessInstanceImpl;
import org.kie.api.KieServices;
import org.kie.api.marshalling.ObjectMarshallingStrategyStore;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.process.ProcessInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * All the ProtobufMessages that make up the bytearray can be found here:
 * https://github.com/kiegroup/jbpm/blob/master/jbpm-flow/src/main/resources/org/jbpm/marshalling/jbpmmessages.proto
 * 
 * So far, I've found the following messages that reference process-instance-id:
 * 
 * ProcessInstance.id ProcessInstance.parent_process_instance_id
 * HumanTaskNode.error_handling_process_instance_id
 * WorkItemNode.error_handling_process_instance_id
 * SubProcessNode.process_instance_id WorkItemNode.process_instances_id
 * 
 * DISCLAIMER: This operation is extremely dangerous! And only tested on very
 * simple use-cases. Currently it is only capable of
 * 
 */
public class PiidMigrationManager {

	private static final Logger LOGGER = LoggerFactory.getLogger(PiidMigrationManager.class);

	public static void main(final String[] args) throws Exception {

		final String url = "jdbc:oracle:thin:@simpsons:1521:cdb1";
		final String user = "c##bart";
		final String password = "bart1987";

		// The id to which I want to change my process instance
		final long processInstanceId = 21L;

		KieServices ks = KieServices.Factory.get();
		KieContainer kContainer = ks.getKieClasspathContainer();
		KieSession kieSession = kContainer.newKieSession();

		LOGGER.debug("Connection to DB!");
		final Connection conn = connect(url, user, password);
		// Need to disable auto-commit in order to work with LargeObjects
		conn.setAutoCommit(false);

		processProcessInstances(processInstanceId, conn, kieSession);
		// processWorkItems(processInstanceId, conn, kieSession);

		conn.close();

	}

	private static void processProcessInstances(long processInstanceId, Connection conn, KieSession kieSession)
			throws Exception {
		final PreparedStatement ps = conn
				.prepareStatement("SELECT processinstancebytearray FROM processinstanceinfo p WHERE p.instanceid = ?");

		ps.setLong(1, processInstanceId);

		LOGGER.debug("Retrieving ProcessInstance bytearray");
		final ResultSet resultSet = ps.executeQuery();

		if (resultSet.next()) {

			int columnIndex = 1;
			
			java.sql.Blob blob = (java.sql.Blob) resultSet.getObject(columnIndex);

			InputStream binaryStream = blob.getBinaryStream();

			byte[] processInstanceByteArray = IOUtils.toByteArray(binaryStream);

			// Unmarshal the byte array into a ProcessInstance object. ProcessInstance
			ProcessInstance processInstance = unmarshalProcessInstance(processInstanceByteArray, kieSession);
			LOGGER.debug("Unmarshalled ProcessInstance with instance-id: " + processInstance.getId());
			
			/*
			 * //Change the processInstance Id ((RuleFlowProcessInstance)
			 * processInstance).setId(processInstanceId);
			 * 
			 * //Marshal the object back into a byte-array. byte[]
			 * marshalledProcessInstanceByteArray = marshalProcessInstance(processInstance,
			 * kieSession);
			 * 
			 * //Write the byte array back to the database. LargeObject objWrite =
			 * lobj.open(oid, LargeObjectManager.WRITE);
			 * objWrite.write(marshalledProcessInstanceByteArray); conn.commit();
			 */

		}
	}

	private static void processWorkItems(long processInstanceId, Connection conn, KieSession kieSession)
			throws Exception {
		final PreparedStatement psWorkItem = conn
				.prepareStatement("SELECT workitembytearray FROM workiteminfo w WHERE w.processinstanceid = ?");
		psWorkItem.setLong(1, processInstanceId);

		LOGGER.debug("Retrieving WorrkItem bytearray");
		final ResultSet resultSetWorkItem = psWorkItem.executeQuery();

		while (resultSetWorkItem.next()) {

			/*
			 * int columnIndex = 1; LargeObjectManager lobj = ((org.postgresql.PGConnection)
			 * conn).getLargeObjectAPI(); long oid = resultSetWorkItem.getLong(columnIndex);
			 * 
			 * if (oid < 1) { throw new RuntimeException("Invalid bytearray object id!"); }
			 * 
			 * LargeObject obj = lobj.open(oid, LargeObjectManager.READ);
			 * 
			 * //Get the WorkItem byte array from the large object byte[] workItemByteArray
			 * = new byte[obj.size()]; obj.read(workItemByteArray, 0, obj.size());
			 * 
			 * //Unmarshall the byte array into a WorkItem. WorkItem workItem =
			 * unmarshalWorkItem(workItemByteArray, kieSession);
			 * LOGGER.debug("Unmarshalled WorkItem with instance-id: " + workItem.getId());
			 * 
			 * //Change ProcessInstanceId. workItem.setProcessInstanceId(processInstanceId);
			 * 
			 * //Marshall the object back into a byte array. byte[]
			 * marshalledWorkItemByteArray = marshalWorkItem(workItem, kieSession);
			 * 
			 * //Write back to the DB. LargeObject objWrite = lobj.open(oid,
			 * LargeObjectManager.WRITE); objWrite.write(marshalledWorkItemByteArray);
			 * conn.commit();
			 */
		}

	}

	public static Connection connect(final String url, final String user, final String password) {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			System.out.println("Connected to Oracle successfully.");
		} catch (final SQLException e) {
			System.out.println(e.getMessage());
		}

		return conn;
	}

	public static ProcessInstance unmarshalProcessInstance(byte[] processInstanceByteArray, KieSession kieSession)
			throws Exception {

		byte[] data = processInstanceByteArray;
		ProcessInstance processInstance = unmarshallProcessInstances(data, kieSession);

		return processInstance;
	}

	public static byte[] marshalProcessInstance(ProcessInstance processInstance, KieSession kieSession)
			throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		String processInstanceType = "RuleFlow";

		try {
			ProcessMarshallerWriteContext context = new ProcessMarshallerWriteContext(baos, null, null, null, null,
					kieSession.getEnvironment());

			context.setProcessInstanceId(processInstance.getId());
			context.setState(processInstance.getState() == ProcessInstance.STATE_ACTIVE
					? ProcessMarshallerWriteContext.STATE_ACTIVE
					: ProcessMarshallerWriteContext.STATE_COMPLETED);

			String processType = ((ProcessInstanceImpl) processInstance).getProcess().getType();
			saveProcessInstanceType(context, processInstance, processType);
			ProcessInstanceMarshaller marshaller = ProcessMarshallerRegistry.INSTANCE.getMarshaller(processType);

			Object result = marshaller.writeProcessInstance(context, processInstance);
			if (marshaller instanceof ProtobufRuleFlowProcessInstanceMarshaller && result != null) {
				JBPMMessages.ProcessInstance _instance = (JBPMMessages.ProcessInstance) result;
				PersisterHelper.writeToStreamWithHeader(context, _instance);
			}
			context.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return baos.toByteArray();

	}

	private static void saveProcessInstanceType(MarshallerWriteContext context, ProcessInstance processInstance,
			String processInstanceType) throws IOException {
		ObjectOutputStream stream = context.stream;
		// saves the processInstance type first
		stream.writeUTF(processInstanceType);
	}

	private static ProcessInstance unmarshallProcessInstances(byte[] marshalledSessionByteArray, KieSession kieSession)
			throws Exception {

		ByteArrayInputStream bais = new ByteArrayInputStream(marshalledSessionByteArray);
		MarshallerReaderContext context = new MarshallerReaderContext(bais,
				(InternalKnowledgeBase) kieSession.getKieBase(), null, null, ProtobufMarshaller.TIMER_READERS,
				kieSession.getEnvironment());

		context.wm = ((StatefulKnowledgeSessionImpl) kieSession).getInternalWorkingMemory();

		// Unmarshall
		ObjectInputStream stream = context.stream;
		String processInstanceType = stream.readUTF();
		LOGGER.debug("ProcessInstanceType: " + processInstanceType);
		
		ProtobufRuleFlowProcessInstanceMarshaller processMarshaller = (ProtobufRuleFlowProcessInstanceMarshaller) ProcessMarshallerRegistry.INSTANCE
				.getMarshaller(processInstanceType);

		ProcessInstance processInstance = null;
		try {
			processInstance = processMarshaller.readProcessInstance(context);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		context.close();
	
		return processInstance;
	}

	public static byte[] marshalWorkItem(WorkItem workItem, KieSession kieSession) {
		long state = (long) workItem.getState();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			MarshallerWriteContext context = new MarshallerWriteContext(baos, (InternalKnowledgeBase) null,
					(InternalWorkingMemory) null, (Map) null, (ObjectMarshallingStrategyStore) null,
					kieSession.getEnvironment());

			ProtobufOutputMarshaller.writeWorkItem(context, workItem);
			context.close();
			byte[] workItemByteArray = baos.toByteArray();
			return workItemByteArray;
		} catch (IOException var3) {
			throw new IllegalArgumentException(
					"IOException while storing workItem " + workItem.getId() + ": " + var3.getMessage());
		}
	}

	public static WorkItem unmarshalWorkItem(byte[] workItemByteArray, KieSession kieSession) {
		WorkItem workItem = null;
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(workItemByteArray);
			MarshallerReaderContext context = new MarshallerReaderContext(bais,
					(InternalKnowledgeBase) kieSession.getKieBase(), (Map) null, (ObjectMarshallingStrategyStore) null,
					(Map) null, kieSession.getEnvironment());

			try {
				workItem = ProtobufInputMarshaller.readWorkItem(context);
			} catch (Exception var8) {
				try {
					context.close();
					bais = new ByteArrayInputStream(workItemByteArray);
					context = new MarshallerReaderContext(bais, (InternalKnowledgeBase) kieSession.getKieBase(),
							(Map) null, (ObjectMarshallingStrategyStore) null, (Map) null, kieSession.getEnvironment());
					workItem = InputMarshaller.readWorkItem(context);
				} catch (IOException var7) {
					LOGGER.error("Unable to read work item with InputMarshaller", var7);
					throw new RuntimeException("Unable to read work item ", var8);
				}
			}
			context.close();
		} catch (IOException var9) {
			var9.printStackTrace();
			throw new IllegalArgumentException("IOException while loading work item: " + var9.getMessage());
		}

		return workItem;
	}

	private static byte[] readBinaryData(String fileName) throws IOException {
		byte[] bytes = Files.readAllBytes(Paths.get(fileName));
		return bytes;
	}

	private static void writeBinaryData(byte[] data, String fileName) throws IOException {
		Files.write(Paths.get(fileName), data);
	}

}
