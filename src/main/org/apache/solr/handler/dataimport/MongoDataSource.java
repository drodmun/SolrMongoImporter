package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * User: James Date: 13/08/12 Time: 18:28 To change this template use File | Settings | File Templates.
 */

public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);

    private MongoCollection<Document> mongoCollection;
    private MongoDatabase mongoDb;
    private MongoClient mongoClient;

    private MongoCursor<Document> mongoCursor;

    @Override
    public void init(Context context, Properties initProps) {
        String databaseName = initProps.getProperty(DATABASE);
        String database_auth = initProps.getProperty(DATABASE_AUTH);
        String host = initProps.getProperty(HOST, "localhost");
        String port = initProps.getProperty(PORT, "27017");
        String username = initProps.getProperty(USERNAME);
        String password = initProps.getProperty(PASSWORD);

        if (databaseName == null) {
            throw new DataImportHandlerException(SEVERE, "Database must be supplied");
        } else if (database_auth == null) {
            database_auth = databaseName;
        }

        try {

            MongoClient mongo;

            if (hasValue(username) && hasValue(password) && hasValue(database_auth)) {
                MongoCredential credential = MongoCredential.createCredential(username, database_auth, password.toCharArray());
                mongo = new MongoClient(new ServerAddress(host, Integer.parseInt(port)), Arrays.asList(credential));
            } else {
                mongo = new MongoClient(host, Integer.parseInt(port));
            }

            this.mongoClient = mongo;
            this.mongoDb = mongo.getDatabase(databaseName);

        } catch (Exception e) {
            throw new DataImportHandlerException(SEVERE, e);
        }
    }

    private boolean hasValue(String str) {
        return str != null && !str.isEmpty();
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {

        Bson queryObject = BasicDBObject.parse(query);
        LOG.debug("Executing MongoQuery: " + query.toString());

        long start = System.currentTimeMillis();
        mongoCursor = this.mongoCollection.find(queryObject).iterator();
        LOG.trace("Time taken for mongo :" + (System.currentTimeMillis() - start));

        ResultSetIterator resultSet = new ResultSetIterator(mongoCursor);
        return resultSet.getIterator();
    }

    public Iterator<Map<String, Object>> getData(String query, String collection) {
        this.mongoCollection = this.mongoDb.getCollection(collection);
        return getData(query);
    }

    private class ResultSetIterator {
        MongoCursor<Document> MongoCursor;

        Iterator<Map<String, Object>> rSetIterator;

        public ResultSetIterator(MongoCursor<Document> mongoCursor) {
            this.MongoCursor = mongoCursor;

            rSetIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return hasnext();
                }

                public Map<String, Object> next() {
                    return getARow();
                }

                public void remove() {/* do nothing */
                }
            };

        }

        public Iterator<Map<String, Object>> getIterator() {
            return rSetIterator;
        }

        private Map<String, Object> getARow() {
            Document mongoObject = getMongoCursor().next();

            Map<String, Object> result = new HashMap<String, Object>();
            Set<String> keys = mongoObject.keySet();
            Iterator<String> iterator = keys.iterator();

            while (iterator.hasNext()) {
                String key = iterator.next();
                Object innerObject = mongoObject.get(key);

                result.put(key, innerObject);
            }

            return result;
        }

        private boolean hasnext() {
            if (MongoCursor == null)
                return false;
            try {
                if (MongoCursor.hasNext()) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (MongoException e) {
                close();
                wrapAndThrow(SEVERE, e);
                return false;
            }
        }

        private void close() {
            try {
                if (MongoCursor != null)
                    MongoCursor.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing result set", e);
            } finally {
                MongoCursor = null;
            }
        }
    }

    private MongoCursor<Document> getMongoCursor() {
        return this.mongoCursor;
    }

    @Override
    public void close() {
        if (this.mongoCursor != null) {
            this.mongoCursor.close();
            ;
        }

        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }

    public static final String DATABASE = "database";
    public static final String DATABASE_AUTH = "databaseAuth";
    public static final String HOST = "host";
    public static final String PORT = "port";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

}
