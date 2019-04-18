var async = require('async')
const AWS = require('aws-sdk')
const DynamodbFactory = require('@awspilot/dynamodb')
DynamodbFactory.config( {empty_string_replace_as: "\0" } );

var DynamoDB;

if (process.env.CW_DYNAMODB_ENDPOINT) {
	DynamoDB = new DynamodbFactory(
		new AWS.DynamoDB({
			endpoint:        process.env.CW_DYNAMODB_ENDPOINT,
			accessKeyId:     process.env.CW_DYNAMODB_KEY,
			secretAccessKey: process.env.CW_DYNAMODB_SECRET,
			region:          process.env.CW_DYNAMODB_REGION,
		})
	)
} else {
	if (process.env.CW_DYNAMODB_KEY) {
		DynamoDB = new DynamodbFactory(
			new AWS.DynamoDB({
				accessKeyId:     process.env.CW_DYNAMODB_KEY,
				secretAccessKey: process.env.CW_DYNAMODB_SECRET,
				region:          process.env.CW_DYNAMODB_REGION,
			})
		)
	} else {
		DynamoDB = new DynamodbFactory(
			new AWS.DynamoDB()
		)
	}
}


var Cloudwatch = function( config ) {
	this.config = typeof config === "object" ? config : {}


}

Cloudwatch.prototype.create_stats_table = function() {
	var $this=this;

	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;
	ddb.query(`
		CREATE PAY_PER_REQUEST TABLE ` + ($this.config.table_name || process.env.CW_DYNAMODB_TABLE) + ` (
			namespace STRING,
			\`date\` STRING,
			PRIMARY KEY ( namespace, \`date\` )
		)
	`, function(err,data) {
		console.log("create table => ", err, data )
	});
	setTimeout(function() {
		var params = {
			TableName: $this.config.table_name || process.env.CW_DYNAMODB_TABLE,
			TimeToLiveSpecification: {
				AttributeName: 'expire_at',
				Enabled: true,
			}
		};
		ddb.client.updateTimeToLive( params , function(err, data) {
			if (err)
				console.log(err)

		})
	},3000)

}

Cloudwatch.prototype.increment_minute = function( namespace, timestamp, metric, value ) {
	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;

	var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*15)).toString()


	var min_update = {
		"TableName": this.config.table_name || process.env.CW_DYNAMODB_TABLE,
		"Key": {
			namespace: { S: namespace },
			date: {S: 'M '+ new Date(timestamp).toISOString().split('T').join(' ').slice(0,16) },
		},
		"AttributeUpdates": {
			"expire_at": {
				"Action": "PUT",
				"Value": { N: expire_at  }, // expire in 15 days
			},
		},
	}

	min_update.AttributeUpdates[metric] = {
		"Action": "ADD",
		"Value": { N: value.toString() },
	}

	ddb.client.updateItem(min_update, function(err) {
		console.log(err ? '☐' : '☑', "increment minute", err )
	})
};

Cloudwatch.prototype.increment_5minute = function( namespace, timestamp, metric, value ) {
	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;

	var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*63)).toString()

	var t = ( new Date(timestamp).toISOString().slice(0,19) +'.000Z').split(' ').join('T')

	var min_update = {
		"TableName": this.config.table_name || process.env.CW_DYNAMODB_TABLE,
		"Key": {
			namespace: { S: namespace },
			date: {S: '5M '+ (new Date(new Date(t).getTime() - (new Date(t).getTime() % (1000*60*5))).toISOString()).slice(0,16).split('T').join(' ') },
		},
		"AttributeUpdates": {
			"expire_at": {
				"Action": "PUT",
				"Value": { N: expire_at }, // expire in 63 days
			},
		},
	}

	min_update.AttributeUpdates[metric] = {
		"Action": "ADD",
		"Value": { N: value.toString() },
	}

	ddb.client.updateItem(min_update, function(err) {
		console.log(err ? '☐' : '☑', "increment 5M", err )
	})
};
Cloudwatch.prototype.increment_hour = function( namespace, timestamp, metric, value ) {
	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;

	var expire_at = (Math.round(new Date().getTime() / 1000) + (60*60*24*455)).toString()


	var min_update = {
		"TableName": this.config.table_name || process.env.CW_DYNAMODB_TABLE,
		"Key": {
			namespace: { S: namespace },
			date: {S: 'H '+ new Date(timestamp).toISOString().split('T').join(' ').slice(0,13) },
		},
		"AttributeUpdates": {
			"expire_at": {
				"Action": "PUT",
				"Value": { N: expire_at  }, // expire in 15 days
			},
		},
	}

	min_update.AttributeUpdates[metric] = {
		"Action": "ADD",
		"Value": { N: value.toString() },
	}

	ddb.client.updateItem(min_update, function(err) {
		console.log(err ? '☐' : '☑', "increment hour", err )
	})
};


Cloudwatch.prototype.putMetricData = function( params, cb ) {
	var $this = this;

	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;


	var namespace='_'
	if (params.Namespace)
		namespace = params.Namespace

	if (this.config.namespace_prefix)
		namespace = this.config.namespace_prefix + namespace

	if (!Array.isArray( params.MetricData ))
		return cb({ errorCode: 'invalid MetricData'})


	async.each(params.MetricData, function(metric, cb ) {

		var payload = {
			namespace: namespace,
			date: 'S ' + new Date(metric.Timestamp).toISOString().substr(0,19).split('T').join(' '),
			expire_at: Math.round(new Date().getTime() / 1000) + (60*60*3) // expire in 3 thours
		}

		payload[ metric.MetricName  ] = DynamoDB.add( metric.Value )

		ddb
			.table( $this.config.table_name || process.env.CW_DYNAMODB_TABLE )
			.insert_or_update(payload, function(err) {

				if (err && err.code === 'ResourceNotFoundException')
					$this.create_stats_table()

				//console.log(err ? '☐' : '☑', "increment ", err )
				cb(err)

			})

		if ( $this.config.streams_enabled === false ) {
			$this.increment_minute ( namespace, new Date(metric.Timestamp).getTime(), metric.MetricName, metric.Value )
			$this.increment_5minute( namespace, new Date(metric.Timestamp).getTime(), metric.MetricName, metric.Value )
			$this.increment_hour   ( namespace, new Date(metric.Timestamp).getTime(), metric.MetricName, metric.Value )
		}
	}, function(err) {
		cb(err)
	})
}


/*
var params = {
	StartTime: new Date( parseInt(event._GET.start) ),
	EndTime:   new Date( parseInt(event._GET.end)   ),

	MetricName: event._GET.metric,
	Namespace: event._GET.namespace,
	Period: parseInt( event._GET.period ), // agregate at 1 day
	Statistics: [ event._GET.statistics  ],  // SampleCount | Average | Sum | Minimum | Maximum,
};

if (event._GET.dimension_name) {
	params.Dimensions = [{
		Name:  event._GET.dimension_name,
		Value: event._GET.dimension_value,
	}]
}
if (event._GET.unit)
	params.Unit = event._GET.unit;

cloudwatch.getMetricStatistics(params, function(err, data) {


	data.Datapoints.map(function(dp) {
		var newdate = new Date(
			new Date( dp.Timestamp ).getTime()
		).toISOString()

		cwseries[ newdate.slice(0, date_slice ) ] += dp[event._GET.statistics];
	})


	cb()
});
*/

Cloudwatch.prototype.getMetricStatistics = function( params, cb ) {
	var $this=this;

	var ddb = this.config.hasOwnProperty('DynamoDB') ? this.config.DynamoDB : DynamoDB;

	var between_start;
	var between_end;

	switch (params.Period ) {
		case 60*60*24: // 24h
			between_start = 'H ' + (new Date(params.StartTime).toISOString().slice(0,13).split('T').join(' ')  )
			between_end   = 'H ' + (new Date(params.EndTime  ).toISOString().slice(0,13).split('T').join(' ')  )
			break;
		case 60*60: // 1h
			between_start = 'M ' + (new Date(params.StartTime).toISOString().slice(0,16).split('T').join(' ')  )
			between_end   = 'M ' + (new Date(params.EndTime  ).toISOString().slice(0,16).split('T').join(' ')  )
			break;

		case 60: // minute
			between_start = 'M ' + (new Date(params.StartTime).toISOString().slice(0,16).split('T').join(' ')  )
			between_end   = 'M ' + (new Date(params.EndTime  ).toISOString().slice(0,16).split('T').join(' ')  )
			break;

	}

	if (!between_start)
		return cb({ errorCode: 'invalid period'})



	ddb
		.table( $this.config.table_name || process.env.CW_DYNAMODB_TABLE )
		.where('namespace').eq(params.Namespace)
		.where('date').between( between_start, between_end )
		.query(function(err, data) {
			if (err)
				console.log("query failed", err )

			var ret = {
				// Label:
				Datapoints: [], // { Timestamp: Sum:  }
			}
			var Datapoints = {}
			console.log("cwmock data.length", data.length)
			console.log("cwmock data", JSON.stringify(data,null,"\t"))


			for (var i = new Date(params.StartTime).getTime();i<= new Date(params.EndTime).getTime(); i+=( 1000 * params.Period ) ) {
				console.log(new Date(i - (i % (1000* params.Period ))).toISOString())
				var date = new Date(i - (i % (1000* params.Period ))).getTime()
				if (!Datapoints.hasOwnProperty(date))
					Datapoints[date] = 0
			}

			data.map(function(d) {
				switch (params.Period ) {
					case 60*60: // 1h

						var date = new Date((d.date.slice(2,15) + ':00:00.000Z').split(' ').join('T')).getTime()
						if (!Datapoints.hasOwnProperty(date))
							Datapoints[date] = 0

						if (d[ params.MetricName ])
							Datapoints[date] += d.hasOwnProperty(params.MetricName) ? d[ params.MetricName ] : 0;

						break;
					case 60: // 1min

						var date = new Date((d.date.slice(2,18) + ':00.000Z').split(' ').join('T')).getTime()
						if (!Datapoints.hasOwnProperty(date))
							Datapoints[date] = 0

						if (d[ params.MetricName ])
							Datapoints[date] += d.hasOwnProperty(params.MetricName) ? d[ params.MetricName ] : 0;

						break;
				}
			})

			Object.keys(Datapoints).map(function(timestamp) {
				ret.Datapoints.push({
					Timestamp: timestamp,
					Sum: Datapoints[timestamp],
				})
			})

			cb(null, ret )
		})

}



module.exports = Cloudwatch;
