package com.tuandai.flume.sink.mongodb.transaction;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;


public class MongoDBTransSink extends AbstractSink implements Configurable {
	private static Logger logger = LoggerFactory.getLogger(MongoDBTransSink.class);

	private static DateTimeParser[] parsers = { DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssz").getParser(),
			DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz").getParser(), };
	public static DateTimeFormatter dateTimeFormatter = new DateTimeFormatterBuilder().append(null, parsers)
			.toFormatter();

	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String AUTHENTICATION_ENABLED = "authenticationEnabled";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String MODEL = "model";
	public static final String DB_NAME = "db";
	public static final String COLLECTION = "collection";
	public static final String NAME_PREFIX = "MongSink_";
	public static final String BATCH_SIZE = "batch";
	public static final String AUTO_WRAP = "autoWrap";
	public static final String WRAP_FIELD = "wrapField";
	public static final String ID_FIELD = "id";
	public static final String TIMESTAMP_FIELD = "time";
	public static final String PK = "_id";
	public static final char ID_SEPARATOR = ':';
	public static final String SERVICE_NAME = "serviceName";
	public static final String TRANS_TYPE = "type";
	public static final String OP_INC = "$inc";
	public static final String OP_SET = "$set";
	public static final String OP_SET_ON_INSERT = "$setOnInsert";

	public static final boolean DEFAULT_AUTHENTICATION_ENABLED = false;
	public static final String DEFAULT_HOST = "localhost";
	public static final int DEFAULT_PORT = 27017;
	public static final String DEFAULT_DB = "events";
	public static final String DEFAULT_COLLECTION = "events";
	public static final int DEFAULT_BATCH = 100;
	public static final String DEFAULT_WRAP_FIELD = "log";
	public static final String DEFAULT_TIMESTAMP_FIELD = null;
	public static final char NAMESPACE_SEPARATOR = '.';
	public static final String OP_UPSERT = "upsert";
	public static final String EXTRA_FIELDS_PREFIX = "extraFields.";

	private static AtomicInteger counter = new AtomicInteger();

	private Mongo mongo;
	private DB db;

	private String host;
	private int port;
	private boolean authentication_enabled;
	private String username;
	private String password;
	private String dbName;
	private String collectionName;
	private int batchSize;
	private String wrapField;
	private String timestampField;
	private final Map<String, String> extraInfos = new ConcurrentHashMap<String, String>();

	@Override
	public void configure(Context context) {
		setName(NAME_PREFIX + counter.getAndIncrement());

		host = context.getString(HOST, DEFAULT_HOST);
		port = context.getInteger(PORT, DEFAULT_PORT);
		authentication_enabled = context.getBoolean(AUTHENTICATION_ENABLED, DEFAULT_AUTHENTICATION_ENABLED);
		if (authentication_enabled) {
			username = context.getString(USERNAME);
			password = context.getString(PASSWORD);
		} else {
			username = "";
			password = "";
		}
		dbName = context.getString(DB_NAME, DEFAULT_DB);
		collectionName = context.getString(COLLECTION, DEFAULT_COLLECTION);
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH);
		wrapField = context.getString(WRAP_FIELD, DEFAULT_WRAP_FIELD);
		timestampField = context.getString(TIMESTAMP_FIELD, DEFAULT_TIMESTAMP_FIELD);
		extraInfos.putAll(context.getSubProperties(EXTRA_FIELDS_PREFIX));
		
		logger.info(
				"MongoSink {} context { host:{}, port:{}, authentication_enabled:{}, username:{}, password:{}, model:{}, dbName:{}, collectionName:{}, batch: {}, autoWrap: {}, wrapField: {}, timestampField: {} }",
				new Object[] { getName(), host, port, authentication_enabled, username, password, dbName,
						collectionName, batchSize, wrapField, timestampField });
	}

	@Override
	public synchronized void start() {
		logger.info("Starting {}...", getName());
		try {
			mongo = new Mongo(host, port);
			db = mongo.getDB(dbName);
		} catch (UnknownHostException e) {
			logger.error("Can't connect to mongoDB", e);
			return;
		}
		if (authentication_enabled) {
			boolean result = db.authenticate(username, password.toCharArray());
			if (result) {
				logger.info("Authentication attempt successful.");
			} else {
				logger.error(
						"CRITICAL FAILURE: Unable to authenticate. Check username and Password, or use another unauthenticated DB. Not starting MongoDB sink.\n");
				return;
			}
		}
		super.start();
		logger.info("Started {}.", getName());
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = Status.BACKOFF;
		try {
			status = parseEvents();
		} catch (Exception e) {
			logger.error("can't process events", e);
		}

		return status;
	}

	private void saveEvents(Map<String, List<DBObject>> eventMap) throws Exception {
		if (eventMap.isEmpty()) {
			logger.debug("eventMap is empty");
			return;
		}

		for (Map.Entry<String, List<DBObject>> entry : eventMap.entrySet()) {
			List<DBObject> docs = entry.getValue();
			if (logger.isDebugEnabled()) {
				logger.debug("collection: {}, length: {}", entry.getKey(), docs.size());
			}
			int separatorIndex = entry.getKey().indexOf(NAMESPACE_SEPARATOR);
			String eventDb = entry.getKey().substring(0, separatorIndex);
			String collectionNameVar = entry.getKey().substring(separatorIndex + 1);

			DB dbRef = mongo.getDB(eventDb);
			if (authentication_enabled) {
				boolean authResult = dbRef.authenticate(username, password.toCharArray());
				if (!authResult) {
					logger.error("Failed to authenticate user: " + username + " with password: " + password
							+ ". Unable to write events.");
					return;
				}
			}
			try {
				CommandResult result = dbRef.getCollection(collectionNameVar).insert(docs, WriteConcern.SAFE)
						.getLastError();
				if (result.ok()) {
					String errorMessage = result.getErrorMessage();
					if (errorMessage != null) {
						logger.error("can't insert documents with error: {} ", errorMessage);
						logger.error("with exception", result.getException());
						throw new MongoException(errorMessage);
					}
				} else {
					logger.error("can't get last error");
				}
			} catch (Exception e) {
				if (e instanceof com.mongodb.MongoException.DuplicateKey) {
					logger.error("can't process event batch : {}, docs: {}", e,docs);
				}else{
					logger.error("can't process event batch : {}", e);
					throw e;
				}
			}
		}
	}

	private Status parseEvents() throws EventDeliveryException {
		Channel channel = getChannel();
		Transaction tx = null;
		Status status = Status.BACKOFF;
		try {
			tx = channel.getTransaction();
			tx.begin();
			Map<String, List<DBObject>> eventMap = new HashMap<String, List<DBObject>>();
			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();
	            if (event == null || event.getBody() == null || event.getBody().length == 0) {
	            	break;
	            } else {
	        		logger.debug("{} start to process event", getName());
	        		addEventToList(eventMap, event);
				}
			}
			if (eventMap.size() > 0) {
				saveEvents(eventMap);
			}
			tx.commit();
            status = Status.READY;
		} catch (Exception e) {
			logger.error("can't process events, rollback!", e);
            tx.rollback();
            status = Status.BACKOFF;
		} finally {
			if (tx != null) {
				tx.close();
			}
		}
		return  status;
	}

	private void addEventToList(Map<String, List<DBObject>> eventMap, Event event) {

		DBObject eventJson;
		byte[] body = event.getBody();

		try {
			eventJson = (DBObject) JSON.parse(new String(body));
		} catch (Exception e) {
			logger.error("Can't parse events: " + new String(body), e);
			return ;
		}

		eventJson.put(TIMESTAMP_FIELD, new Date());

		for (Map.Entry<String, String> entry : extraInfos.entrySet()) {
			eventJson.put(entry.getKey(), entry.getValue());
		}

		// TODO general ID
		if (!eventJson.containsField(ID_FIELD)) {
			logger.error("Can't find id: " + eventJson.toString());
			return ;
		}
		if (!eventJson.containsField(SERVICE_NAME)) {
			logger.error("Can't find service name: " + eventJson.toString());
			return ;
		}
		String pkStr = eventJson.get(SERVICE_NAME).toString() + ID_SEPARATOR + eventJson.get(ID_FIELD).toString();
		eventJson.put(PK, pkStr);
		eventJson.removeField(ID_FIELD);
		eventJson.removeField(SERVICE_NAME);

		// 初始化 collection
		String eventCollection = dbName + NAMESPACE_SEPARATOR + collectionName;		
		if (eventJson.containsField(TRANS_TYPE)) {			
			eventCollection = eventCollection + TRANS_TYPE ;
			if (!eventMap.containsKey(eventCollection)) {
				eventMap.put(eventCollection, new ArrayList<DBObject>());
			}
		}else{
			if (!eventMap.containsKey(eventCollection)) {
				eventMap.put(eventCollection, new ArrayList<DBObject>());
			}
		}
		
		// get document
		List<DBObject> documents = eventMap.get(eventCollection);
		documents = eventMap.get(eventCollection);
		
		if (documents == null) {
			documents = new ArrayList<DBObject>(batchSize);
		}
		// add  doc
		documents.add(eventJson);

		
	}
}
