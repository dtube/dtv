var steem = require('steem')
steem.api.setOptions({ url: 'https://api.steemit.com' });
var mysql = require('mysql')
var express = require('express')
var app = express()
var getJSON = require('get-json')

var bodyParser = require('body-parser');
app.use(bodyParser.text({ type: 'text/plain' }))

var sql = mysql.createConnection({
    host     : 'localhost',
    user     : 'root',
    password : '',
    database : 'dtube'
});
sql.connect();
clearUnverifiedKeys()
 
app.get('/getStreams', function (req, res) {
    var command = {
        active_streams: [
            "clients",
            "lastms"
        ]
    }
    getJSON('http://localhost:4242/api?command='+JSON.stringify(command), function(err, jRes) {
        res.send(jRes.active_streams)
    })
})

app.get('/resetKey/:username', function (req, res) {
    keyLine = {
        username: req.params.username, 
        streamKey: createKey(16), 
        verifKey: createKey(16)
    }
    var query = mysql.format('INSERT INTO streamKeys SET ?', keyLine)
    sql.query(query, function(err, qRes, fields) {
        if (err) throw err;
        res.send(keyLine)
    })
})

app.get('/verify/:block/:tx_num', function (req, res) {
    steem.api.getBlock(req.params.block, function(err, sRes) {
        var tx = sRes.transactions[req.params.tx_num]
        var op = tx.operations[0]
        if (op[0] !== "custom_json") {
            res.send('Nope')
            return
        }
        if (op[1].id != "dtubeStreamVerif") {
            res.send('Nope')
            return
        }

        try {
            var json = JSON.parse(op[1].json)
        } catch (error) {
            res.send('Nope')
            return
        }

        var query = 'SELECT * FROM streamKeys WHERE verifKey="'
                    +json.key
                    +'" AND username="'
                    +op[1].required_posting_auths[0]
                    +'" AND verified=0'
        sql.query(query, function(err, qRes, fields) {
            if (err) throw err;
            if (qRes.length < 1) {
                res.send('Couldnt find matching stream key')
                return
            }
            var query = 'UPDATE streamKeys SET verified=1 WHERE verifKey="'+json.key+'"'
            sql.query(query, function(err, qRes, fields) {
                if (err) throw err;
                var query = 'UPDATE streamKeys SET verified=0 WHERE username = "'+op[1].required_posting_auths[0]+'" AND verifKey != "'+json.key+'"'
                sql.query(query, function(err, qRes, fields) {
                    if (err) throw err;
                    res.send('Ok')
                })
            })
        })        
    })
})

app.post('/rtmpRewrite', function (req, res) {
    var params = req.body.split('\n')

    // only allow this call from mistserver in local
    if (req.ip != '::1' && req.ip != '127.0.0.1') {
        res.send('Nope')
        return
    } else {
        var streamKey = params[0].split('/')[params[0].split('/').length-1]
        sql.query('SELECT username FROM streamKeys WHERE verified=1 AND streamKey = "'+streamKey+'"', function(err, qRes, fields) {
            if (err) throw err;
            if (qRes.length < 1) {
                res.send('Nope')
            } else {
                console.log(qRes[0].username+' started streaming.')
                res.send('rtmp://'+params[0].split('/')[2]+'/live/normal+'+qRes[0].username)
            }
        })
        
    }
    
})
 
app.listen(3000)

function createKey(length) {
    var text = "";
    var possible = "abcdefghijklmnopqrstuvwxyz0123456789";
  
    for (var i = 0; i < length; i++)
      text += possible.charAt(Math.floor(Math.random() * possible.length));
  
    return text;
}

function clearUnverifiedKeys() {
    setInterval(function() {
        var query = 'DELETE FROM streamKeys WHERE verified = 0 AND generatedOn < (NOW() - INTERVAL 10 MINUTE)'
        sql.query(query, function(err, qRes, fields) {
            if (err) throw err;
            if (qRes.affectedRows > 0)
                console.log(qRes.affectedRows+' unverified keys removed')
        })
    }, 10*60*1000)
}