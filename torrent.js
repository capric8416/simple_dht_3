var fs = require('fs');
var rimraf = require('rimraf');
var clivas = require('clivas');
var program = require('commander');
var ws = require("nodejs-websocket");
var WebTorrent = require('webtorrent');

require('events').EventEmitter.prototype._maxListeners = 100;


const CONFIG = JSON.parse(fs.readFileSync('config.json', {'flag': 'r', 'encoding': 'utf-8'}));
const ACTION = CONFIG['web_socket']['keys']['action'];
const DATA = CONFIG['web_socket']['keys']['data'];
const POSTFIX = CONFIG['web_socket']['keys']['postfix'];
const POSTFIX_DATABASE = CONFIG['web_socket']['postfixes']['database'];
const POSTFIX_TABLE = CONFIG['web_socket']['postfixes']['table'];
const TASK_OPEN = CONFIG['web_socket']['actions']['task_open'];
const TASK_INFO = CONFIG['web_socket']['actions']['task_info'];
const TASK_CLOSE = CONFIG['web_socket']['actions']['task_close'];


class FetchTorrentMetadata {
    constructor(web_socket, magnet_prefix, info_hashes, {pool_size = 10, max_connections = 32, interval = 1000, timeout = 30000} = {}) {
        this.web_socket = web_socket;

        this.magnet_prefix = magnet_prefix;
        this.info_hashes = info_hashes;

        this.pool_size = pool_size;
        this.max_connections = max_connections;

        this.interval = interval < timeout ? interval : timeout;
        this.loops = 0;
        this.timeout = timeout;

        this.web_torrent = new WebTorrent();

        this.all = this.info_hashes.length;
        this.finished = 0;
        this.timeouts = 0;

        this.database_postfix = info_hashes[0].slice(0, 2);
        this.table_postfix = info_hashes[0].slice(2, 4);
    }

    start() {
        this.web_torrent.on('error', (error) => {
            if (error) {
                clivas.line('web_torrent: {red:Error:} ' + (error.message || error));
                process.exit(1);
            }
        });

        for (let i = 0; i < this.pool_size; i++) {
            let info_hash = this.info_hashes.shift();
            if (!info_hash) {
                break;
            }

            this.add_task(info_hash);
        }

        this.add_watcher();
    }

    add_watcher() {
        var watcher = setInterval(() => {
            this.loops += 1;

            let running = [];
            let torrents = this.web_torrent.torrents.sort(function (a, b) {
                return b.numPeers - a.numPeers || a.timeout - b.timeout;
            });
            for (let torrent of torrents) {
                running.push(`${ torrent.numPeers }:${ torrent.timeout / 1000 }`);

                if (this.elapsed(torrent.creation_timestamp) >= torrent.timeout) {
                    let delay = (torrent.numPeers + 1) * this.timeout;
                    if (delay > torrent.timeout) {
                        torrent.timeout = delay;
                    }
                    else {
                        if (!torrent.metadata) {
                            // clivas.line('{bold:%s} {red:timeout:} %ss', torrent.infoHash, torrent.timeout / 1000);
                            this.timeouts += 1;
                            this.remove_task(torrent);
                            this.next_task();
                        }
                    }
                }
            }

            // clivas.clear();
            clivas.line(
                '{cyan:elapsed:} {8+bold:%ss} {green:all - finished - timeouts:} {24+bold:%s - %s - %s = %s } ' +
                '{green:running:} %s',
                this.loops * this.interval / 1000, this.all, this.finished,
                this.timeouts, this.all - this.finished - this.timeouts, running.join(' ')
            );

            if (this.is_finished()) {
                clearInterval(watcher);
                let message_close = {};
                message_close[ACTION] = TASK_CLOSE;
                message_close[POSTFIX] = {};
                message_close[POSTFIX][POSTFIX_DATABASE] = this.database_postfix;
                message_close[POSTFIX][POSTFIX_TABLE] = this.table_postfix;
                this.web_socket.sendText(JSON.stringify(message_close));
                setTimeout(() => this.web_socket.close(), 1000);
            }
        }, this.interval);
    }

    add_task(info_hash) {
        info_hash = info_hash.toLowerCase();

        // clivas.line('{cyan+bold:%s}', info_hash);

        let torrent = this.web_torrent.add(this.get_torrent_id(info_hash), {maxWebConns: this.max_connections});
        torrent.timeout = this.timeout;
        torrent.creation_timestamp = FetchTorrentMetadata.get_current_timestamp();

        // torrent.on('infoHash', () => this.info_hash_received(torrent.infoHash));
        //
        // torrent.on('wire', () => this.info_hash_received(torrent.infoHash));

        torrent.once('metadata', () => this.metadata_received(torrent.infoHash));
    }

    // info_hash_received(info_hash) {
    //     let torrent = this.get_torrent(info_hash);
    //     if (!torrent) {
    //         return;
    //     }
    //
    //     clivas.line('{bold:%s} {yellow:peers:} %s #%ss', torrent.infoHash, torrent.numPeers, torrent.timeout / 1000);
    // }

    metadata_received(info_hash) {
        let torrent = this.get_torrent(info_hash);
        if (!torrent) {
            return;
        }

        this.finished += 1;

        let message_info = {};
        message_info[ACTION] = TASK_INFO;
        message_info[DATA] = this.parse_metadata(torrent);
        // message_info[POSTFIX] = {};
        // message_info[POSTFIX][POSTFIX_DATABASE] = this.database_postfix;
        // message_info[POSTFIX][POSTFIX_TABLE] = this.table_postfix;
        this.web_socket.sendText(JSON.stringify(message_info));

        this.remove_task(torrent);

        this.next_task()
    }

    static get_current_timestamp() {
        return (new Date()).valueOf();
    }

    get_torrent(info_hash) {
        return this.web_torrent.get(this.get_torrent_id(info_hash));
    }

    parse_metadata(torrent) {
        let info_hash = torrent.infoHash.toLowerCase();

        let files = [];
        torrent.files.forEach((file) => {
            let path = file.path;
            if (path.startsWith(torrent.name + '/')) {
                path = path.substr(torrent.name.length + 1);
            }
            files.push({'path': path, 'size': file.length});
        });

        let metadata = {
            'info_hash': info_hash,
            'name': torrent.name,
            'size': torrent.length,
            'files': files,
            'peers': torrent.numPeers,
            'released': null
        };

        // clivas.line(
        //     '{bold:%s} {green:metadata:} {red+inverse:%s / %s}',
        //     torrent.infoHash, this.finished, this.all
        // );
        // console.log(JSON.stringify(metadata));

        return metadata;
    }

    remove_task(torrent) {
        torrent.removeAllListeners();

        let path = torrent.path;
        torrent.destroy(() => {
            // clivas.line('{bold:%s} {blue:directory:} %s', torrent.infoHash, path);
            this.remove_directory(path);
        });
    }

    next_task() {
        let next_info_hash = this.info_hashes.shift();
        if (next_info_hash) {
            this.add_task(next_info_hash);
        }

        if (this.is_finished() && this.web_torrent) {
            this.web_torrent.destroy();
        }
    }

    elapsed(milliseconds) {
        return FetchTorrentMetadata.get_current_timestamp() - milliseconds;
    }

    is_finished() {
        return (this.finished == this.all || this.finished + this.timeouts === this.all);
    }

    remove_directory(path) {
        rimraf(path, (error) => {
            if (error) {
                clivas.line('remove_directory: {red:Error:} ' + error);
            }
        });
    }

    get_torrent_id(info_hash) {
        if (info_hash.startsWith(this.magnet_prefix)) {
            return info_hash.toLowerCase();
        }
        return this.magnet_prefix + info_hash.toLowerCase();
    }
}


program.version('0.0.1').option('-p, --port <n>', 'port', parseInt).parse(process.argv);
if (!program.port) {
    process.exit(1);
}


ws.createServer(function (connection) {
    clivas.line('{80+magenta+inverse:Connection opened}');

    connection.on('text', function (message) {
        let body = JSON.parse(message);
        let action = body[ACTION];
        if (action == TASK_OPEN) {
            let fetcher = new FetchTorrentMetadata(connection, 'magnet:?xt=urn:btih:', body[DATA]);
            fetcher.start();
        }

    });

    connection.on('close', function (code, reason) {
        clivas.line('{80+magenta+inverse:Connection closed}')
    })
}).listen(program.port);



