var db = require('../models/database.js');


var inputUsername = req.session.user;
if (inputUsername == null) {
	console.log('no username to look up restaurants with');
	res.redirect('/');
} else {
	// scan in all restaurant data
	db.getAllRests(function(data, err) {
		if (err) {
			res.render('restaurants.ejs', {
				message : err,
				user : inputUsername,
				rests : null
			});
		} else if (data) {
			// var body = data;
			  // res.setHeader('Content-Type', 'text/plain');
			  // res.setHeader('Content-Length', body.length);
			  // res.end(body);
			mapData = data; 
			res.render('restaurants.ejs', {
				message : null,
				user : inputUsername,
				rests : data
			});
		} else {
			console.log('rest case 3 in routes');
			res.render('restaurants.ejs', {
				rests : null,
				user : inputUsername,
				message : 'We did not find anything'
			});
		}
	});
}