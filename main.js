var  webmq   = require('./lib/webMQ0');
//var hdfsoper = require('./lib/webHDFS');
var hdfsoper  = require('./lib/hdfsoper');
//hdfs.uploadToHDFS('./file/','www.js', '/home/','www.js');
//hdfs.downloadFromHDFS('/home/','www.js', './ggg/','www.js');
//hdfs.deleteFile('/home/', 'www.js');

//var hdfs = hdfsoper.createClient(
//    {
//        user: 'root',
//        host: '172.27.8.110',
//        port: 50070,
//        path: '/webhdfs/v1'
//    }
//);

//hdfs.uploadToHDFS('./file/','www.js', '/home/','www.js');
//hdfs.chown('ggg','755','/home/', 'www.js');
//hdfs.getFIleStatus('/home/','www.js',function(err,data){
//    console.log(data);
//});

hdfsoper.downloadFromHDFS('/home/','www.js', './ggg/','45.js');
