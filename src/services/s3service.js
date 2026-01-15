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

        // Pipeline configurations
        this.pipelines = {
            orders: {
                sourcePath: config.aws.sourcePath,
                processedPath: config.aws.processedPath,
                logsPath: config.aws.logsPath
            },
            labProduct: {
                sourcePath: config.aws.lab_product_sourcepath,
                processedPath: config.aws.lab_product_processedPath,
                logsPath: config.aws.lab_product_logsPath
            },
            labPractice: {
                sourcePath: config.aws.lab_practice_sourcepath,
                processedPath: config.aws.lab_practice_processedPath,
                logsPath: config.aws.lab_practice_logsPath
            },
            labProductMapping: {
                sourcePath: config.aws.lab_product_mapping_sourcepath,
                processedPath: config.aws.lab_product_mapping_processedPath,
                logsPath: config.aws.lab_product_mapping_logsPath
            },
            labPracticeMapping: {
                sourcePath: config.aws.lab_practice_mapping_sourcepath,
                processedPath: config.aws.lab_practice_mapping_processedPath,
                logsPath: config.aws.lab_practice_mapping_logsPath
            },
            dentalGroups: {
                sourcePath: config.aws.dental_groups_sourcepath,
                processedPath: config.aws.dental_groups_processedPath,
                logsPath: config.aws.dental_groups_logsPath
            }
        };

        // Keep backward compatibility
        this.sourcePath = this.pipelines.orders.sourcePath;
        this.processedPath = this.pipelines.orders.processedPath;
        this.logsPath = this.pipelines.orders.logsPath;
        this.lab_product_sourcepath = this.pipelines.labProduct.sourcePath;
        this.lab_product_processedPath = this.pipelines.labProduct.processedPath;
        this.lab_product_logsPath = this.pipelines.labProduct.logsPath;
        this.lab_practice_sourcepath = this.pipelines.labPractice.sourcePath;
        this.lab_practice_processedPath = this.pipelines.labPractice.processedPath;
        this.lab_practice_logsPath = this.pipelines.labPractice.logsPath;
        this.lab_product_mapping_sourcepath = this.pipelines.labProductMapping.sourcePath;
        this.lab_product_mapping_processedPath = this.pipelines.labProductMapping.processedPath;
        this.lab_product_mapping_logsPath = this.pipelines.labProductMapping.logsPath;
        this.lab_practice_mapping_sourcepath = this.pipelines.labPracticeMapping.sourcePath;
        this.lab_practice_mapping_processedPath = this.pipelines.labPracticeMapping.processedPath;
        this.lab_practice_mapping_logsPath = this.pipelines.labPracticeMapping.logsPath;
        this.dental_groups_sourcepath = this.pipelines.dentalGroups.sourcePath;
        this.dental_groups_processedPath = this.pipelines.dentalGroups.processedPath;
        this.dental_groups_logsPath = this.pipelines.dentalGroups.logsPath;
    }

    /**
     * Check if file exists in S3
     */
    async checkFileExists(fileName, sourcePath) {
        try {
            const command = new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: sourcePath + fileName,
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
    async getFile(fileName, sourcePath) {
        try {
            const command = new GetObjectCommand({
                Bucket: this.bucket,
                Key: sourcePath + fileName
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
     * Generic: List all CSV files in a path (with pagination support)
     */
    async _listFiles(sourcePath, processedPath) {
        try {
            const allFiles = [];
            let continuationToken = undefined;

            do {
                const command = new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: sourcePath,
                    ContinuationToken: continuationToken
                });

                const response = await this.client.send(command);

                if (response.Contents && response.Contents.length > 0) {
                    const files = response.Contents
                        .filter(obj => {
                            return obj.Key !== sourcePath &&
                                !obj.Key.startsWith(processedPath) &&
                                obj.Key.endsWith('.csv');
                        })
                        .map(obj => obj.Key.replace(sourcePath, ''));

                    allFiles.push(...files);
                }

                continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
            } while (continuationToken);

            return allFiles;
        } catch (error) {
            logger.error('Error listing files', { sourcePath, error: error.message });
            throw error;
        }
    }

    /**
     * Generic: Move file to processed folder
     */
    async _moveToProcessed(fileName, sourcePath, processedPath) {
        try {
            const sourceKey = sourcePath + fileName;
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const destKey = processedPath + `${timestamp}_${fileName}`;

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
            await this.client.send(deleteCommand);
            logger.info('Original file deleted', { sourceKey });

            return destKey;
        } catch (error) {
            logger.error('Error moving file', { fileName, error: error.message });
            throw error;
        }
    }

    /**
     * Generic: Upload log file to S3
     */
    async _uploadLogFile(localLogPath, fileName, logsPath) {
        try {
            const fileContent = await fs.readFile(localLogPath);
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const s3Key = `${logsPath}${fileName}_${timestamp}`;

            const command = new PutObjectCommand({
                Bucket: this.bucket,
                Key: s3Key,
                Body: fileContent,
                ContentType: 'text/plain'
            });

            await this.client.send(command);
            logger.info('Log file uploaded to S3', { localPath: localLogPath, s3Key, bucket: this.bucket });

            return s3Key;
        } catch (error) {
            logger.error('Error uploading log file to S3', { localPath: localLogPath, error: error.message });
            throw error;
        }
    }

    // ==================== Orders Pipeline ====================

    async listFiles() {
        const { sourcePath, processedPath } = this.pipelines.orders;
        return this._listFiles(sourcePath, processedPath);
    }

    async moveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.orders;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async uploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.orders;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }

    // ==================== Lab Product Pipeline ====================

    async listLabProductFiles() {
        const { sourcePath, processedPath } = this.pipelines.labProduct;
        return this._listFiles(sourcePath, processedPath);
    }

    async labProductMoveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.labProduct;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async labProductUploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.labProduct;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }

    // ==================== Lab Practice Pipeline ====================

    async listLabPracticeFiles() {
        const { sourcePath, processedPath } = this.pipelines.labPractice;
        return this._listFiles(sourcePath, processedPath);
    }

    async labPracticeMoveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.labPractice;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async labPracticeUploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.labPractice;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }

    // ==================== Lab Product Mapping Pipeline ====================

    async listLabProductMappingFiles() {
        const { sourcePath, processedPath } = this.pipelines.labProductMapping;
        return this._listFiles(sourcePath, processedPath);
    }

    async labProductMappingMoveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.labProductMapping;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async labProductMappingUploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.labProductMapping;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }

    // ==================== Lab Practice Mapping Pipeline ====================

    async listLabPracticeMappingFiles() {
        const { sourcePath, processedPath } = this.pipelines.labPracticeMapping;
        return this._listFiles(sourcePath, processedPath);
    }

    async labPracticeMappingMoveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.labPracticeMapping;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async labPracticeMappingUploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.labPracticeMapping;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }

    // ==================== Dental Groups Pipeline ====================

    async listDentalGroupsFiles() {
        const { sourcePath, processedPath } = this.pipelines.dentalGroups;
        return this._listFiles(sourcePath, processedPath);
    }

    async dentalGroupsMoveToProcessed(fileName) {
        const { sourcePath, processedPath } = this.pipelines.dentalGroups;
        return this._moveToProcessed(fileName, sourcePath, processedPath);
    }

    async dentalGroupsUploadLogFile(localLogPath, fileName) {
        const { logsPath } = this.pipelines.dentalGroups;
        return this._uploadLogFile(localLogPath, fileName, logsPath);
    }
}

module.exports = S3Service;