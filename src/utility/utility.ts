import { environment } from "../environment/environment";
import fetch from "node-fetch";
import HttpException from "../exceptions/http/http-base-exception";
import { Readable } from "stream";
import { FileEntity } from "nodets-ms-core/lib/core/storage";

export class Utility {

    public static async generateSecret(): Promise<string> {
        let secret = null;
        try {
            const result = await fetch(environment.secretGenerateUrl as string, {
                method: 'get'
            });

            if (result.status != undefined && result.status != 200)
                throw new Error(await result.text());

            const data = await result.text();

            secret = data;
        } catch (error: any) {
            console.error(error);
            throw new HttpException(400, "Failed to generate secret token");
        }
        return secret;
    }
}

/**
 * Stream reader for FileEntity. Needed for zip download of
 * the files.
 */
export class FileEntityStream extends Readable {
    constructor(private fileEntity: FileEntity) {
        super();
    }

    async _read(size: number): Promise<void> {
        const fileStream = await this.fileEntity.getStream();

        fileStream.on('data', (chunk) => {
            if (!this.push(chunk)) {
                // If the internal buffer is full, pause until the consumer is ready
                fileStream.pause();
            }
        });

        fileStream.on('end', () => {
            this.push(null); // No more data to push
        });

        fileStream.on('error', (err) => {
            this.emit('error', err);
        });
    }
}
