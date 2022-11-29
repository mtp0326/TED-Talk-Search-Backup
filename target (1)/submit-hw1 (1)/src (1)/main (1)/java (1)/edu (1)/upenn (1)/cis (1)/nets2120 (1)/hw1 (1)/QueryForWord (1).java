package edu.upenn.cis.nets2120.hw1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class QueryForWord {
	/**
	 * A logger is useful for writing different types of messages
	 * that can help with debugging and monitoring activity.  You create
	 * it and give it the associated class as a parameter -- so in the
	 * config file one can adjust what messages are sent for this class. 
	 */
	static Logger logger = LogManager.getLogger(QueryForWord.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	
	/**
	 * Inverted index
	 */
	Table iindex;
	
	Stemmer stemmer;

	/**
	 * Default loader path
	 */
	public QueryForWord() {
		stemmer = new PorterStemmer();
	}
	
	/**
	 * Initialize the database connection
	 * 
	 * @throws IOException
	 */
	public void initialize() throws IOException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		logger.debug("Connected!");
		iindex = db.getTable("inverted");
	}
	
	public Set<Set<String>> query(final String[] words) throws IOException, DynamoDbException, InterruptedException {
		Set<Set<String>> product = new HashSet<>();
		
		for(int i=0;i<words.length;i++) {
			boolean willRemove = false;
			int counter = 0;
			 while(counter < words[i].length() && willRemove == false) {
				if(words[i].charAt(counter) < 'A' || (words[i].charAt(counter) > 'Z'
						&& words[i].charAt(counter) < 'a')
						|| words[i].charAt(counter) > 'z') {
					willRemove = true;
				}
				counter++;
			}
			
			if(willRemove) {
				words[i] = "";
			} else {
				words[i] = stemmer.stem(words[i].toLowerCase()).toString();
				
				if(words[i].equals("a") || words[i].equals("all")
						|| words[i].equals("any") || words[i].equals("but")
						|| words[i].equals("the")) {
					willRemove = true;
				}
				
				if(willRemove) {
					words[i] = "";
				} else {
					Set<String> url = new HashSet<>();
					KeyAttribute key = new KeyAttribute("keyword", words[i]);
					ItemCollection<QueryOutcome> items = iindex.query(key);
					
					Iterator<Item> iterator = items.iterator();
					Item item = null;
					while(iterator.hasNext()) {
						item = iterator.next();
						url.add(item.getString("url"));
					}
					product.add(url);
				}
			}
		}
		return product;
	}

	/**
	 * Graceful shutdown of the DynamoDB connection
	 */
	public void shutdown() {
		logger.info("Shutting down");
		DynamoConnector.shutdown();
	}

	public static void main(final String[] args) {
		final QueryForWord qw = new QueryForWord();

		try {
			qw.initialize();

			final Set<Set<String>> results = qw.query(args);
			for (Set<String> s : results) {
				System.out.println("=== Set");
				for (String url : s)
				  System.out.println(" * " + url);
			}
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			qw.shutdown();
		}
	}
}