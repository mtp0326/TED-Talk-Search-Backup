/**
 * 
 */

var port = 8081;

const express = require('express');
const app = new express();
const path = require("path");
const stemmer = require("stemmer");

const AWS = require("aws-sdk");

const {DynamoDB, QueryCommand } = require('@aws-sdk/client-dynamodb-v2-node');

//AWS.config.loadFromPath('./config.json');
AWS.config.update({region: 'us-east-1'});

const client = new AWS.DynamoDB();

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"))

app.get('/', function(request, response) {
    response.sendFile('html/index.html', { root: __dirname });
});

app.get('/bear.jpg', function(request, response) {
    response.sendFile('html/bear.jpg', { root: __dirname });
});

app.get('/talks', function(request, response) {
  	var docClient = new AWS.DynamoDB.DocumentClient();

	const init = stemmer(request.query.keyword.toLowerCase());
	
	let results = new Array;
	// Set the parameters
	const params = {
		KeyConditionExpression: "keyword = :k",
		ExpressionAttributeValues: {
		    ":k": init,
		},
		TableName: "inverted",
	};
	
	docClient.query(params, function(err, data) {
		
		if (err) {
			console.log(err, err.stack);
			results.push("err");
		}
		
		else {
			let wordCount = 0;
			while(wordCount < 15) {
				console.log(data.Items[wordCount].url);
				results.push(data.Items[wordCount].url);
				wordCount++;
			}
			response.render("results", { "search": request.query.keyword, "results": results });
		}
	});
	
	console.log(results);
  
  // TODO look up the word (or, in the case of EC2, words) in 'terms' in DynamoDB and hand them over as the variable 'results' below
  
});

app.listen(port, () => {
  console.log(`HW1 app listening at http://localhost:${port}`)
})
