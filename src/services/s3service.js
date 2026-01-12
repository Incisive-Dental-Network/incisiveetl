const {
    S3Client,
    GetObjectCommand,
    CopyObjectCommand,
    DeleteObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand
} = require('@aws-sdk/client-s3');
const config = require('../config');
const logger = require('../utils/logger');
const fs = require('fs/promises');

class S3Service {
    constructor() {
        const s3Config = { region: config.aws.region };

        if (config.aws.credentials.accessKeyId && config.aws.credentials.secretAccessKey) {
            s3Config.credentials = config.aws.credentials;
            logger.info('Using AWS credentials from environment variables');
        } else {
            logger.info('Using AWS credentials from default credential chain');
        }

        this.client = new S3Client(s3Config);
        this.bucket = config.aws.bucket;
        this.sourcePath = config.aws.sourcePath;
        this.processedPath = config.aws.processedPath;
        this.logsPath = config.aws.logsPath
    }

    /**
     * Check if file exists in S3
     */
    async checkFileExists(fileName) {
        try {
            const command = new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: this.sourcePath + fileName,
                MaxKeys: 1
            });

            const response = await this.client.send(command);
            return response.Contents && response.Contents.length > 0;
        } catch (error) {
            logger.error('Error checking file existence', { fileName, error: error.message });
            throw error;
        }
    }

    /**
     * Get file from S3
     */
    async getFile(fileName) {
        try {
            const command = new GetObjectCommand({
                Bucket: this.bucket,
                Key: this.sourcePath + fileName
            });

            const response = await this.client.send(command);
            logger.info('Successfully retrieved file from S3', { fileName });
            return response.Body;
        } catch (error) {
            logger.error('Error getting file from S3', { fileName, error: error.message });
            throw error;
        }
    }

    /**
     * List all CSV files in source folder
     */
    async listFiles() {
        try {
            const command = new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: this.sourcePath
            });

            const response = await this.client.send(command);

            if (!response.Contents || response.Contents.length === 0) {
                return [];
            }

            return response.Contents
                .filter(obj => {
                    return obj.Key !== this.sourcePath &&
                        !obj.Key.startsWith(this.processedPath) &&
                        obj.Key.endsWith('.csv');
                })
                .map(obj => obj.Key.replace(this.sourcePath, ''));
        } catch (error) {
            logger.error('Error listing files', { error: error.message });
            throw error;
        }
    }

    /**
     * Move processed file to processed folder
     */
    async moveToProcessed(fileName) {
        try {
            const sourceKey = this.sourcePath + fileName;
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const destKey = this.processedPath + `${timestamp}_${fileName}`;

            // Copy file
            const copyCommand = new CopyObjectCommand({
                Bucket: this.bucket,
                CopySource: `${this.bucket}/${sourceKey}`,
                Key: destKey
            });
            await this.client.send(copyCommand);
            logger.info('File copied to processed folder', { sourceKey, destKey });

            // Delete original
            const deleteCommand = new DeleteObjectCommand({
                Bucket: this.bucket,
                Key: sourceKey
            });
            // await this.client.send(deleteCommand);********
            logger.info('Original file deleted', { sourceKey });

            return destKey;
        } catch (error) {
            logger.error('Error moving file', { fileName, error: error.message });
            throw error;
        }
    }

    async uploadLogFile(localLogPath, fileName) {
        try {
            const fileContent = await fs.readFile(localLogPath);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const s3Key = `${this.logsPath}${fileName}_${timestamp}`;

            const command = new PutObjectCommand({
                Bucket: this.bucket,
                Key: s3Key,
                Body: fileContent,
                ContentType: 'text/plain'
            });

            await this.client.send(command);
            logger.info('Log file uploaded to S3', {
                localPath: localLogPath, 
                s3Key,
                bucket: this.bucket
            });

            return s3Key;
        } catch (error) {
            logger.error('Error uploading log file to S3', {
                localPath: localLogPath,
                error: error.message
            });
            throw error;
        }
    }
}

module.exports = S3Service;