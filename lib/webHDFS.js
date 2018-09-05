/**
 * Created by kf6444 on 2016/12/15.
 */
var http    = require('http');
var request = require('request');
var fs      = require('fs');
var extend  = require('extend');
var util    = require('util');
var path    = require('path');
function webHDFS(opts, requestParams) {
    if (!(this instanceof webHDFS)) {
        return new webHDFS(opts, requestParams);
    }

    [ 'user', 'host', 'port', 'path' ].some(function iterate (property) {
        if (!opts.hasOwnProperty(property)) {
            throw new Error(
                util.format('Unable to create webHDFS client: missing option %s', property)
            );
        }
    });
    this._requestParams = requestParams;
    this._opts = opts;
    this._url = {
        protocol: opts.protocol || 'http',
        hostname: opts.host,
        port: parseInt(opts.port) || 80,
        pathname: opts.path
    };
}
/** 文件上传
 *
 * @param srcPath 要上传的文件所在本地路径
 * @param srcFileaName 文件名
 * @param desPath 文件上传后的存放路径 相对 /v1/目录
 * @param desFileName 文件上传后的文件名
 */
webHDFS.prototype.uploadToHDFS = function (srcPath, srcFileaName, desPath, desFileName) {
    var hdfsConnOption = this._url;
    var hdfsurl = hdfsConnOption.protocol+ '://' + hdfsConnOption.hostname + ':' + hdfsConnOption.port + hdfsConnOption.pathname + desPath + desFileName + '?op=CREATE&overwrite=true';
    console.log(hdfsurl);
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
 * @param srcPath 文件在hdfs服务器上路径, 相对/v1/目录
 * @param srcFileName 要下载的文件名
 * @param desPath 下载后本地存放路径
 * @param desFileName 下载后的文件名
 */

webHDFS.prototype.downloadFromHDFS = function (srcPath, srcFileName, desPath, desFileName) {
    var hdfsConnOption = this._url;
    var hdfsurl = hdfsConnOption.protocol+ '://' + hdfsConnOption.hostname + ':' + hdfsConnOption.port + hdfsConnOption.pathname + srcPath + srcFileName + '?op=OPEN';

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
 * @param filePath 文件在hdfs服务器路径，相对/v1/目录
 * @param fileName 文件名
 */
webHDFS.prototype.deleteFile = function (filePath, fileName) {
    var hdfsConnOption = this._url;
    var option = {
        hostname: hdfsConnOption.hostname,
        port: hdfsConnOption.port,
        path: hdfsConnOption.pathname + filePath + fileName + '?op=DELETE&recursive=true',
        method: 'DELETE'
    };
    console.log(option);
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

/** 文件拥有者与权限设置 [先设置拥有者 再设值权限]
 * @param owner 文件所属用户名
 * @param octal文件权限
 * @param filePath 文件路径 相对'/v1' 目录
 * @param fileName 文件名
 * @param group[可选] 文件所属用户组
 */
webHDFS.prototype.chown = function (owner, octal, filePath, fileName, group) {
    var hdfsConnOption = this._url;
    var option = {
        hostname: hdfsConnOption.hostname,
        port: hdfsConnOption.port,
        method: 'PUT'
    };
    var query = '?op=SETOWNER&user.name=root&owner='+owner + (group== null ?(''):('&group='+group));
    option.path = hdfsConnOption.pathname + filePath + fileName + query;
    console.log(option);
    http.request(option, function (res) {
        var str = '';
        res.on('data', function (chunk) {
            str += chunk;
        });
        res.on('end', function () {
            console.log('Set owner over', 'filename: ' + fileName);
            option.path = hdfsConnOption.pathname + filePath + fileName + '?op=SETPERMISSION&user.name=root&permission='+octal;
            console.log(option);
            http.request(option, function(res){
                var str = '';
                res.on('data', function(chunk) {
                    str += chunk;
                });
                res.on('end', function(){
                    console.log('Set chmod over', 'filename: ' + fileName);
                });
                res.on('error', function(err) {
                    console.log('Set chmod with err: ' + err, 'filename: ' + fileName);
                });
            }).end();
        });
        res.on('error', function (err) {
            console.log('Set owner with err: ' + err, 'filename: ' + fileName);
        });
    }).end();
};

/** 获取文件状态包括owner 和 permission
 * @param filePath 文件在hdfs服务器路径，相对/v1/目录
 * @param fileName 文件名
 */
webHDFS.prototype.getFileStatus = function (filePath, fileName, callback) {
    var hdfsConnOption = this._url;
    var option = {
        hostname: hdfsConnOption.hostname,
        port: hdfsConnOption.port,
        path: hdfsConnOption.pathname + filePath + fileName + '?op=GETFILESTATUS',
        method: 'GET'
    };
    http.request(option, function (res) {
        var str = '';
        res.on('data', function (chunk) {
            str += chunk;
        });
        res.on('end', function () {
            console.log('Get file status over', 'filename: ' + fileName);
            callback(null, str);
        });
        res.on('error', function (err) {
            console.log('Get file status with err: ' + err, 'filename: ' + fileName);
            callback(err, null);
        });
    }).end();
};

module.exports = {
    createClient: function createClient (opts, requestParams) {
        return new webHDFS(extend({
            user: 'root',
            host: 'localhost',
            port: '50070',
            path: '/webhdfs/v1'
        }, opts), requestParams);
    }
};
