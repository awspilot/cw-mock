describe('getMetricStatistics', function () {
	it('putMetricData', function(done) {
		var params = {
			MetricData:[{
				MetricName: 'sent',
				Timestamp:  new Date,
				Value: 1,
			}],
			Namespace: 'hello/world/2',
		};
		cloudwatch.putMetricData(params,function(err,data) {
			console.log(err ? '☐' : '☑', "putMetricData.sent", err )
			if (err)
				throw err;

			done()
		});
	})

	// it('getMetricStatistics', function(done) {
	// 
	// 	var params = {
	// 		Namespace: 'hello/world',
	// 
	// 		StartTime: new Date( '2019-01-18T00:00:00.000Z' ),
	// 		EndTime:   new Date( '2019-02-20T23:59:00.000Z' ),
	// 
	// 		MetricName: 'sent',
	// 
	// 		Period: 60*60*24, // in seconds 
	// 		Statistics: [ 'Sum' ],  // SampleCount | Average | Sum | Minimum | Maximum,
	// 	};
	// 
	// 	cloudwatch.getMetricStatistics( params, function( err, data ) {
	// 		console.log(err ? '☐' : '☑', "getMetricStatistics.sent", err )
	// 		done()
	// 	})
	// })

})