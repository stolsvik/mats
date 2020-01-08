const http = require('http');
const fs = require('fs');
const path = require('path');
const opn = require('opn');

// Node http server, derived from: https://developer.mozilla.org/en-US/docs/Learn/Server-side/Node_server_without_framework

http.createServer(function (request, response) {
    const extname = String(path.extname(request.url)).toLowerCase();
    // Look for .js files in ../lib, and others in ./static
    let basePath = (extname === '.js') ? '../lib' : 'static';

    // Resolve / to index.html, other urls are used as is
    let file = (request.url === '/') ? '/index.html' : request.url;

    // Resolve the path relative to this script dir
    let filePath = path.resolve(__dirname, basePath + file);

    console.log('request %s => %s', request.url, filePath);

    const mimeTypes = {
        '.html': 'text/html',
        '.js': 'text/javascript',
        '.css': 'text/css',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.wav': 'audio/wav',
        '.mp4': 'video/mp4',
        '.woff': 'application/font-woff',
        '.ttf': 'application/font-ttf',
        '.eot': 'application/vnd.ms-fontobject',
        '.otf': 'application/font-otf',
        '.wasm': 'application/wasm'
    };

    const contentType = mimeTypes[extname] || 'application/octet-stream';

    fs.readFile(filePath, function(error, content) {
        if (error) {
            if(error.code == 'ENOENT') {
                response.writeHead(404, { 'Content-Type': 'text/plain' });
                response.end("Not found", 'utf-8');
            }
            else {
                response.writeHead(500);
                response.end('Sorry, check with the site admin for error: '+error.code+' ..\n');
            }
        }
        else {
            response.writeHead(200, { 'Content-Type': contentType });
            response.end(content, 'utf-8');
        }
    });

}).listen(8125);
console.log('Server running at http://127.0.0.1:8125/');
opn('http://127.0.0.1:8125/index.html');