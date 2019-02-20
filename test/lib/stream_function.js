
var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB({
	endpoint:        process.env.CW_DYNAMODB_ENDPOINT,
	accessKeyId:     process.env.CW_DYNAMODB_KEY,
	secretAccessKey: process.env.CW_DYNAMODB_SECRET,
	region:          process.env.CW_DYNAMODB_REGION,
})

var increment_minute = function(rec,old_rec, cb ) {
		var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*15)).toString()
		var min_update = {
			"TableName": process.env.CW_DYNAMODB_TABLE,
			"Key": {
				namespace: rec.namespace,
				date: {S: 'M'+ rec.date.S.slice(1,18) },
			},
			"AttributeUpdates": {
				"expire_at": {
					"Action": "PUT",
					"Value": { N: expire_at  }, // expire in 15 days
				},
			},
		}
		Object.keys(rec).map(function( attr ) {
			if ( attr === 'expire_at') return;
			if ( rec[attr].hasOwnProperty('N') )
				min_update.AttributeUpdates[attr] = {
					"Action": "ADD",
					"Value": {N: old_rec === null ? rec[attr].N : (parseFloat(rec[attr].N) - parseFloat(old_rec[attr].N || 0)).toString()},  
				}
		})
		dynamodb.updateItem(min_update, function(err) {
			console.log(err ? '☐' : '☑', "increment M", err )
			cb(err)
		})
};

var increment_5minute = function(rec,old_rec, cb ) {
		var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*63)).toString()
		var timestamp = (rec.date.S.slice(2)+'.000Z').split(' ').join('T')
		var min_update = {
			"TableName": process.env.CW_DYNAMODB_TABLE,
			"Key": {
				namespace: rec.namespace,
				date: {S: '5M '+ (new Date(new Date(timestamp).getTime() - (new Date(timestamp).getTime() % (1000*60*5))).toISOString()).slice(0,16).split('T').join(' ') },
			},
			"AttributeUpdates": {
				"expire_at": {
					"Action": "PUT",
					"Value": { N: expire_at }, // expire in 63 days
				},
			},
		}
		Object.keys(rec).map(function( attr ) {
			if ( attr === 'expire_at') return;
			if ( rec[attr].hasOwnProperty('N') )
				min_update.AttributeUpdates[attr] = {
					"Action": "ADD",
					"Value": {N: old_rec === null ? rec[attr].N : (parseFloat(rec[attr].N) - parseFloat(old_rec[attr].N || 0)).toString()},
				}
		})
		dynamodb.updateItem(min_update, function(err) {
			console.log(err ? '☐' : '☑', "increment 5M", err )
			cb(err)
		})
};

var increment_hour = function(rec,old_rec, cb ) {
		var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*455)).toString()
		var min_update = {
			"TableName": process.env.CW_DYNAMODB_TABLE,
			"Key": {
				namespace: rec.namespace,
				date: {S: 'H'+ rec.date.S.slice(1,15) },
			},
			"AttributeUpdates": {
				"expire_at": {
					"Action": "PUT",
					"Value": { N: expire_at  }, // expire in 455 days
				},
			},
		}
		Object.keys(rec).map(function( attr ) {
			if ( attr === 'expire_at') return;
			if ( rec[attr].hasOwnProperty('N') )
				min_update.AttributeUpdates[attr] = {
					"Action": "ADD",
					"Value": {N: old_rec === null ? rec[attr].N : (parseFloat(rec[attr].N) - parseFloat(old_rec[attr].N || 0)).toString()},
				}
		})
		dynamodb.updateItem(min_update, function(err) {
			console.log(err ? '☐' : '☑', "increment H", err )
			cb(err)
		})
};

exports.handler = function(event, context) {
	
	var ev =      event.Records[0].eventName
	var rec =     (event.Records[0].dynamodb.NewImage || {})
	var old_rec = (event.Records[0].dynamodb.OldImage || {})
	console.log("on", event.Records[0].eventName )
	if (((rec.date || {}).S || [])[0] !== 'S')
		return context.done();
	
	if ( ev === 'INSERT' ) {
		increment_minute(rec, null,function(err) {
			increment_5minute(rec,null, function(err) {
				increment_hour(rec,null, function(err) {
					context.done()
				})
			})
		})
		return;
	};
	
	if ( ev === 'MODIFY' ) {
		increment_minute(rec, old_rec,function(err) {
			increment_5minute(rec,old_rec, function(err) {
				increment_hour(rec,old_rec, function(err) {
					context.done()
				})
			})
		})
		return;
	};
	
	console.log(JSON.stringify(event,null,"\t"))
	context.done();

	
	
};
