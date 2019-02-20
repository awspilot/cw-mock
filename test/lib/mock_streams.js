

var Timestamp = new Date().getTime()/1000;
var dynamodbStreams;
var StreamArn;
var StreamDescription;
var process_after_timestamp= new Date().getTime() - (1000*60*1) // dynamodb record;s ApproximateCreationDateTime are rounded to 1 min
var StartingSequenceNumber;
var Shards = {};


function stream_consumer( cb ) {
	
	//console.log("----stream----")

	var iterators = [];
	var Records = [];

	async.waterfall([




		function( cb ) {
			dynamodbStreams.describeStream({ StreamArn: StreamArn }, function(err, data ) {
				if (err)
					return cb(err)


				StreamDescription = data.StreamDescription;
				//Shards = StreamDescription.Shards
				
				StreamDescription.Shards.map(function(s) {
					if (Shards.hasOwnProperty(s.ShardId)) {
						Shards[s.ShardId].Shard = s
					} else {
						Shards[s.ShardId] = {
							Shard: s,
							CurrentSequenceNumber: s.SequenceNumberRange.EndingSequenceNumber || s.SequenceNumberRange.StartingSequenceNumber
						};
						//console.log("CurrentSequenceNumber",)
					}
				})

				// Object.keys(Shards).map(function(sk) {
				// 	console.log("CurrentSequenceNumber=",JSON.stringify(Shards[sk].CurrentSequenceNumber,null,"\t"))
				// })

				cb()
			})
		},
		
		function( cb ) {
			async.eachSeries(Object.keys(Shards), function( sk, cb ) {
		
				//console.log(JSON.stringify(Shards[sk],null,"\t"))
		
				var params = {
					ShardIteratorType: 'TRIM_HORIZON', // LATEST, TRIM_HORIZON, AT_TIMESTAMP, AT_SEQUENCE_NUMBER
					ShardId: sk,
					StreamArn: StreamArn,
				}
				if (Shards[sk].CurrentSequenceNumber) {
					//console.log("CurrentSequenceNumber",Shards[sk].CurrentSequenceNumber) // sequence is per shard
					params = {
						ShardIteratorType: 'AFTER_SEQUENCE_NUMBER', // AFTER_SEQUENCE_NUMBER, AT_SEQUENCE_NUMBER
						SequenceNumber: Shards[sk].CurrentSequenceNumber,
						ShardId: sk,
						StreamArn: StreamArn,
					}
				} else {
					console.log("check this", JSON.stringify(Shards[sk],null,"\t"))
				}
				dynamodbStreams.getShardIterator(params, function(err, data) {
					if (err)
						return cb(err)
		
					//iterators.push(data.ShardIterator)
					Shards[sk].ShardIterator = data.ShardIterator

					cb()
				})
			}, function(err) {
				cb(err)
			})
		},

		// for each shard, get records but process only one
		function( cb ) {
			async.eachSeries(Object.keys(Shards), function( sk, cb ) {

				//console.log("ShardIterator=", Shards[sk].ShardIterator )

				dynamodbStreams.getRecords({ ShardIterator: Shards[sk].ShardIterator}, function( err, data ) {
					//if (err) console.log(err)
					if (err)
						return cb()
					
					if (!data.Records.length)
						return cb()

					var record_to_process = null

					for (i = 0; i < data.Records.length; i++) {
						if (process_after_timestamp > new Date(data.Records[i].dynamodb.ApproximateCreationDateTime).getTime() ) {
							//console.log(i, Shards[sk].CurrentSequenceNumber ,"OOOOLD")
							Shards[sk].CurrentSequenceNumber = data.Records[i].dynamodb.SequenceNumber
						} else {
							//console.log(i, Shards[sk].CurrentSequenceNumber ,"VEWWW")
							Shards[sk].CurrentSequenceNumber = data.Records[i].dynamodb.SequenceNumber
							record_to_process = data.Records[0]
							break
						}
					}
					

					if (record_to_process) {
						try {
							require('./stream_function').handler(
								{
									Records: [ record_to_process ]
								},
								{
									done: function(err,data) {
										cb()
									},
									failed: function(err) {
										cb()
									},
									succeed: function(err) {
										cb()
									},
								},
							);
						} catch(err){
							console.log(err)
							cb()
						}
						return;
					}

					cb()
				})

			}, function(err) {
				cb(err)
			})
		},

	], function(err) {
		if (err) console.log(err)
		cb()
	})
}


async.waterfall([

	function( cb ) {
		dynamodbStreams = new AWS.DynamoDBStreams({
			endpoint:        process.env.CW_DYNAMODB_ENDPOINT,
			accessKeyId:     process.env.CW_DYNAMODB_KEY,
			secretAccessKey: process.env.CW_DYNAMODB_SECRET,
			region:          process.env.CW_DYNAMODB_REGION,
		});
		cb()
	},

	function( cb ) {
		dynamodbStreams.listStreams({},function(err, data) {
			if (err)
				return cb(err)

			StreamArn = (data.Streams.filter(function(s) { return s.TableName === process.env.CW_DYNAMODB_TABLE })[0] || {}).StreamArn
			
			if (!StreamArn)
				return cb('stream-not-found')

			cb()
		})
	},

	function( cb ) {
		setInterval(function() {
			stream_consumer( function() {
			})
		}, 1000)
	},

	
], function(){
	
})










