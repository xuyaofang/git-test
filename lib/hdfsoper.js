/**
 * Created by kf6444 on 2016/12/15.
 */
var http    = require('http');
var request = require('request');
var fs      = require('fs');
var path    = require('path');
var config  = require('config') ;

var hdfsConnOption = config.get('webHDFS');

function hdfsoper() {


}
/** 文件上传
 *
 * @param srcPath 要上传的文件所在本地路径
 * @param srcFileaName 文件名
 * @param desPath 文件上传后的存放路径 相对 /v1/目录
 * @param desFileName 文件上传后的文件名
 */
hdfsoper.prototype.uploadToHDFS = function (srcPath, srcFileaName, desPath, desFileName) {

    var hdfsurl = 'http://' + hdfsConnOption.host + ':' + hdfsConnOption.port + hdfsConnOption.path + desPath + desFileName + '?op=CREATE&overwrite=true';
    var hdfs1 = request.put(hdfsurl);
    hdfs1.on('response', function (response) {
        var rdfs = fs.createReadStream(srcPath + srcFileaName);
        if (307 == response.statusCode) {
            console.log('307');
            var options = {
                url: response.headers.location,
                method: 'PUT',
                headers: {
                    'User-Agent': 'request',
                    'content-type': 'application/octet-stream'
                }
            };
            var writeHdfs = request(options);
            writeHdfs.on('end', function () {
                console.log('Upload over','filename: ' + srcFileaName);
            });
            rdfs.on('end', function () {
                console.log('Read over','filename: ' + srcFileaName);
            });
            rdfs.on('readable', function () {
                console.log('Readable happen','filename: ' + srcFileaName);
            });
            rdfs.on('data', function (chunk) {
                console.log('Get %d bytes', chunk.length,'filename: ' + srcFileaName);
            });
            rdfs.on('close', function () {
                console.log('Read close','filename: ' + srcFileaName);
            });
            rdfs.on('error', function (error) {
                console.log('createReadStream with error: ' + error,'filename: ' + srcFileaName);
            });
            writeHdfs.on('error', function (err) {
                console.log('Upload with error: ' + err,'filename: ' + srcFileaName);
            });
        }
        rdfs.pipe(writeHdfs);
    });
};

/** 文件下载
 * @param srcPath 要下载的文件在hdfs服务器上路径, 相对/v1/目录
 * @param srcFileName 要下载的文件名
 * @param desPath 文件下载后本地存放路径
 * @param desFileName 文件下载后的文件名
 */

hdfsoper.prototype.downloadFromHDFS = function (srcPath, srcFileName, desPath, desFileName) {
    var hdfsurl = 'http://' + hdfsConnOption.host + ':' + hdfsConnOption.port + hdfsConnOption.path + srcPath + srcFileName + '?op=OPEN&user.name=root';
    if (!fs.existsSync(desPath)) {
        mkdirs(desPath, 0777, function (pathdir){
            _upoad();
        });
    } else {
        _upoad();
    }
    //创建多级文件目录
    function mkdirs(dirpath, mode, callback) {
        fs.exists(dirpath, function(exists){
            if (exists) {
                callback(dirpath);

            } else {
                mkdirs(path.dirname(dirpath), mode, function(){
                    fs.mkdir(dirpath, mode, callback);
                });
            }
        });
    }
    function _upoad() {
        var hdfs1 = request.get(hdfsurl);

        var writelocal = fs.createWriteStream(desPath + desFileName, {flags: 'w+'});

        hdfs1.pipe(writelocal);

        writelocal.on('error', function (err) {
            console.error('Download with error: ' + err,'filename: ' + srcFileName);
        });
        writelocal.on('close', function () {
            console.log('Download close','filename: ' + desFileName);
        });
    }
};

/** 文件删除
 * @param filePath 要下载的文件在hdfs服务器路径，相对/v1/目录
 * @param fileName 要删除的文件名
 */
hdfsoper.prototype.deleteFile = function (filePath, fileName) {
    var hdfsurl = 'http://' + hdfsConnOption.host + ':' + hdfsConnOption.port + hdfsConnOption.path + filePath + fileName + '?op=DELETE&recursive=true';
    var option = {
        hostname: hdfsConnOption.host,
        port: hdfsConnOption.port,
        path: hdfsConnOption.path + filePath + fileName + '?op=DELETE&recursive=true',
        method: 'DELETE'
    };
    http.request(option, function (res) {
        var str = '';
        res.on('data', function (chunk) {
            str += chunk;
        });
        res.on('end', function () {
            console.log('Delete over', 'filename: ' + fileName);
        });
        res.on('error', function (err) {
            console.log('Delete request err: ' + err, 'filename: ' + fileName);
        });
    }).end();
};

var hdfs = module.exports = new hdfsoper;

//hdfs.uploadToHDFS('./file/','www.js', '/home/','www.js');
//hdfs.downloadFromHDFS('/home/','www.js', './ggg/','www.js');
//hdfs.deleteFile('/home/', 'www.js');