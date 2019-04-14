module.exports = {
	extract_param: function ( pname, obj ) {
		Object.keys(obj).map(function( k ) {
			var re = new RegExp("^" + pname + '\.member\.([0-9]+)$', "i");
			var arre = k.match(re)
			if ( arre ) {
				if (!obj.hasOwnProperty(pname))
					obj[pname] = []

				obj[pname][parseInt(arre[1])-1] = obj[ k ]
				delete obj[ k ];
				return;
			}

			var re = new RegExp("^" + pname + '\.member\.([0-9]+)\.([a-z]+)$', "i")
			var objre = k.match(re)
			if ( objre ) {
				if (!obj.hasOwnProperty(pname))
					obj[pname] = []

				if (! (typeof obj[pname][parseInt(objre[1])-1] === "object") )
					obj[pname][parseInt(objre[1])-1] = {}

				obj[pname][parseInt(objre[1])-1][ objre[2] ] = obj[ k ]
				delete obj[ k ];
			}

		})
	}
}
