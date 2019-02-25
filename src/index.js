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

Cloudwatch.prototype.putMetricData = function( params, cb ) {
	var $this = this;

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

		DynamoDB
			.table( $this.config.table_name || process.env.CW_DYNAMODB_TABLE)
			.insert_or_update(payload, function(err) {
				//console.log(err ? '☐' : '☑', "increment ", err )
				cb(err)

			})
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

	}
	
	if (!between_start)
		return cb({ errorCode: 'invalid period'})
	
	
console.log('cwmock',"start=", between_start, "end=", between_end )
	
	DynamoDB
		.table( $this.config.table_name || process.env.CW_DYNAMODB_TABLE)
		.where('namespace').eq(params.Namespace)
		.where('date').between( between_start, between_end )
		.query(function(err, data) {
			
			var ret = {
				// Label:
				Datapoints: [], // { Timestamp: Sum:  }
			}
			var Datapoints = {}
			data.map(function(d) {
				switch (params.Period ) {
					case 60*60: // 1h
						var date = new Date((d.date.slice(2,15) + ':00:00.000Z').split(' ').join('T')).getTime()
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
			//console.log( err, data )
			cb(null, ret )
		})

}



module.exports = Cloudwatch;
