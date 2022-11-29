package edu.upenn.cis.nets2120.hw1;

import java.util.Arrays;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import edu.upenn.cis.nets2120.hw1.files.TedTalkParser.TalkDescriptionHandler;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Callback handler for talk descriptions.  Parses, breaks words up, and
 * puts them into DynamoDB.
 * 
 * @author zives
 *
 */
public class IndexTedTalkInfo implements TalkDescriptionHandler {
	static Logger logger = LogManager.getLogger(TalkDescriptionHandler.class);

  final static String tableName = "inverted";
	int row = 0;
	
	SimpleTokenizer model;
	Stemmer stemmer;
	DynamoDB db;
	Table iindex;
	
	public IndexTedTalkInfo(final DynamoDB db) throws DynamoDbException, InterruptedException {
		model = SimpleTokenizer.INSTANCE;
		stemmer = new PorterStemmer();
		this.db = db;

		initializeTables();
	}

	/**
	 * Called every time a line is read from the input file. Breaks into keywords
	 * and indexes them.
	 * 
	 * @param csvRow      Row from the CSV file
	 * @param columnNames Parallel array with the names of the table's columns
	 */
	@Override
	public void accept(final String[] csvRow, final String[] columnNames) {
		ArrayList<String> map = new ArrayList<String>();
		ArrayList<Item> queue = new ArrayList<Item>();
		for(int i=0;i<columnNames.length;i++) {
	
				
			if(columnNames[i].equals("title") || columnNames[i].equals("speaker_1")
					|| columnNames[i].equals("all_speakers")
					|| columnNames[i].equals("occupations")
					|| columnNames[i].equals("about_speakers")
					|| columnNames[i].equals("topics")
					|| columnNames[i].equals("description")
					|| columnNames[i].equals("transcript")
					|| columnNames[i].equals("related_talks")) {
				String[] temp = model.tokenize(csvRow[i]);
				for(int j=0;j<temp.length;j++) {
					map.add(temp[j]);
				}
			}
		}
		
		for(int i=0;i<map.size();i++) {
			String word = map.get(i);
			boolean willRemove = false;
			int counter = 0;
			 while(counter < word.length() && willRemove == false) {
				if(word.charAt(counter) < 'A' || (word.charAt(counter) > 'Z'
						&& word.charAt(counter) < 'a')
						|| word.charAt(counter) > 'z') {
					willRemove = true;
				}
				counter++;
			}
			
			if(willRemove) {
				map.remove(i);
				i--;
			} else {
				map.set(i, stemmer.stem(map.get(i).toLowerCase()).toString());
				
				
				if(map.get(i).equals("a") || map.get(i).equals("all")
						|| map.get(i).equals("any") || map.get(i).equals("but")
						|| map.get(i).equals("the")) {
					willRemove = true;
				}
				
				if(willRemove) {
					map.remove(i);
					i--;
				} else {
					word = map.get(i);
					map.remove(i);

					if(!map.contains(word)) {
						Item item = new Item()
								.withPrimaryKey("keyword", word)
								.withString("url", lookup(csvRow, columnNames, "url"))
								.withPrimaryKey("inxid", Integer.parseInt(lookup
										(csvRow, columnNames, "talk_id")));

						queue.add(item);
						if(queue.size() == 25) {
							TableWriteItems tableWriteItems = new TableWriteItems
									(tableName).withItemsToPut(queue);
							BatchWriteItemOutcome outcome = db.batchWriteItem(tableWriteItems);
							
							while(outcome.getUnprocessedItems().size() != 0) {
								outcome = db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
							}
							queue.removeAll(queue);
						}
						
					}
					map.add(i, word);
				}
			}
		}
		if(queue.size() != 0) {
			TableWriteItems tableWriteItems = new TableWriteItems
					(tableName).withItemsToPut(queue);
			BatchWriteItemOutcome outcome = db.batchWriteItem(tableWriteItems);
			
			while(outcome.getUnprocessedItems().size() != 0) {
				outcome = db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
			}
		}
	}

	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			iindex = db.createTable(tableName, Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH), // Partition
																												// key
					new KeySchemaElement("inxid", KeyType.RANGE)), // Sort key
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),
							new AttributeDefinition("inxid", ScalarAttributeType.N)),
					new ProvisionedThroughput(100L, 100L));

			iindex.waitForActive();
		} catch (final ResourceInUseException exists) {
			iindex = db.getTable(tableName);
		}

	}

	/**
	 * Given the CSV row and the column names, return the column with a specified
	 * name
	 * 
	 * @param csvRow
	 * @param columnNames
	 * @param columnName
	 * @return
	 */
	public static String lookup(final String[] csvRow, final String[] columnNames, final String columnName) {
		final int inx = Arrays.asList(columnNames).indexOf(columnName);
		
		if (inx < 0)
			throw new RuntimeException("Out of bounds");
		
		return csvRow[inx];
	}
}
