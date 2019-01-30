var AWS = require('aws-sdk');
var s3scan = require('s3scan');
var zlib = require('zlib');
var stream = require('stream');
var main = require('./index');

module.exports = function(config, done) {
    var log = config.log || console.log;

    if (!config.source || !config.source.bucket || !config.source.prefix)
        return done(new Error('Must provide source bucket and prefix to snapshot'));

    if (!config.destination || !config.destination.bucket || !config.destination.key)
        return done(new Error('Must provide destination bucket and key where the snapshot will be put'));

    if (!config.transform) {
        config.transform = function(data, enc, callback) {
            if (!data) return callback();
            callback(null, data.Body.toString() + '\n');
        };
    }

    var s3Options = {
        httpOptions: {
            maxRetries: 10,
            timeout: 10000,
            connectTimeout: 4000,
            agent: main.agent
        }
    };
    if (config.maxRetries) s3Options.maxRetries = config.maxRetries;
    if (config.logger) s3Options.logger = config.logger;

    var s3 = new AWS.S3(s3Options);

    var size = 0;
    var srcUri = ['s3:/', config.source.bucket, config.source.prefix].join('/');
    var dstUri = ['s3:/', config.destination.bucket, config.destination.key].join('/');

    var objStream = s3scan.Scan(srcUri, { s3: s3 })
        .on('error', function(err) { done(err); });
    var gzip = zlib.createGzip()
        .on('error', function(err) { done(err); });

    var stringify = new stream.Transform();
    stringify._writableState.objectMode = true;
    stringify._transform = config.transform;

    if (config.tagset)
    {
        // The upload() method Tagging parameter requires a valid URI formatted string
        var tagging = config.tagset.map(function(t){ return t.Key + '=' + encodeURIComponent(t.Value); }).join('&');
    }

    var uploadParams = {
        ServerSideEncryption: process.env.ServerSideEncryption || 'AES256',
        SSEKMSKeyId: process.env.SSEKMSKeyId,
        ContentEncoding: 'gzip',
        ContentType: config.contentType || 'application/json',
        Tagging: tagging,
        Bucket: config.destination.bucket,
        Key: config.destination.key,
        Body: gzip
    };
    var uploadOptions = {
        partSize: 1 * 1024 * 1024
    };
    var upload = s3.upload(uploadParams, uploadOptions).on('httpUploadProgress', function(details) {
        log(
            'Upload part #%s, %s bytes, %s items @ %s items/s to %s',
            details.part, details.loaded, objStream.got, objStream.rate(),
            dstUri
        );
        size = details.loaded;
    }).on('error', function(err) { done(err); });

    log(
        'Starting snapshot from s3://%s/%s to s3://%s/%s',
        config.source.bucket, config.source.prefix,
        config.destination.bucket, config.destination.key
    );

    objStream.pipe(stringify).pipe(gzip);

    upload.send(function(err) {
        if (err) return done(err);
        log('Wrote %s items and %s bytes to %s', objStream.got, size, dstUri);
        done(null, { size: size, count: objStream.got, rate: objStream.rate(), source: srcUri, destination: dstUri });
    });
};
