# cw-mock

subproject of @awspilot/dynamodb-ui  
exposes cloudwatch metrics compatible api  
uses dynamodb as storage  

```
	CwMock = require("@awspilot/cw-mock")

	dbcloudwatch = new CwMock({
		table_name: 'stats',
	});
```