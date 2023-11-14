import * as pg from 'pg'
import { from as copyFrom } from 'pg-copy-streams' // Extension of pg module which allows to use the Copy functionality of PostgreSQL

import { pipeline } from 'node:stream/promises'
import { Transform } from 'node:stream'
import * as fs from 'node:fs'
import * as path from 'node:path';

(async () => {
    const pool = new pg.Pool({
        host: '127.0.0.1',
        port: 5432,
        database: 'TEST',
        user: 'TEST',
        password: 'TEST'
    })

    const client = await pool.connect()
    console.log(__dirname);
    try {
        const ingestStream = client.query(copyFrom("COPY users(id, name, surname, city) FROM STDIN (FORMAT CSV)"));
        const sourceStream = fs.createReadStream(path.resolve(__dirname, '..', 'users.csv'))
        await pipeline(sourceStream, transformFullname(), ingestStream)
    } finally {
        client.release()
    }

    await pool.end()
})()


function transformFullname() {
    let unprocessed = '';
    let newChunkString = '';
    const transformer = new Transform({
        transform(chunk, encoding, callback) {
            let chunkString = unprocessed + chunk.toString()
            unprocessed = '';
            newChunkString = '';

            let startIndex = 0;
            for (let ch = startIndex; ch < chunkString.length; ch++) {
                if (chunkString[ch] === '\n') {
                    const line = chunkString.slice(startIndex, ch)

                    newChunkString += splitFullname(line);

                    startIndex = ch + 1;
                }
            }

            if (chunkString[chunkString.length - 1] !== '\n') {
                unprocessed = chunkString.slice(startIndex);
            }

            callback(null, newChunkString)
        },
        flush(callback) {
            if (unprocessed) {

                callback(null, splitFullname(unprocessed))
            }
        }
    });

    return transformer;
}

function splitFullname(line: string): string {
    const lineSplit = line.split(',');

    if (lineSplit[0] !== 'ID') {
        // Not header

        const nameParts = lineSplit[1].split(' ');

        const id = lineSplit[0];
        const name = nameParts[0];
        const surname = nameParts[1];
        const city = lineSplit[2].substring(0, lineSplit[2].length - 1); // remove \n at the end

        return `"${id}", "${name}", "${surname}", "${city}"\n`
    }

    return '';
}


