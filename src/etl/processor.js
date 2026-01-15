const logger = require('../utils/logger');
const fs = require('fs/promises');
const path = require('path');
const { objectToCSV } = require('../utils/csv-helpers');

class ETLProcessor {
    constructor(s3Service, dbService, csvProcessor) {
        this.s3Service = s3Service;
        this.dbService = dbService;
        this.csvProcessor = csvProcessor;
    }

    /**
     * Generate detailed summary report for log file
     * NEW METHOD
     */
    generateSummaryReport(fileName, results) {
        const { rowCount, successCount, errorCount, missingFieldErrors, duration } = results;

        let report = '\n' + '='.repeat(80) + '\n';
        report += 'ETL PROCESSING SUMMARY REPORT\n';
        report += '='.repeat(80) + '\n\n';
        report += `File Name: ${fileName}\n`;
        report += `Processing Date: ${new Date().toISOString()}\n`;
        report += `Duration: ${(duration / 1000).toFixed(2)} seconds\n\n`;

        report += 'STATISTICS:\n';
        report += '-'.repeat(80) + '\n';
        report += `Total Rows in CSV: ${rowCount}\n`;
        report += `Successfully Processed: ${successCount}\n`;
        report += `Failed/Skipped: ${errorCount}\n`;
        report += `Success Rate: ${rowCount > 0 ? ((successCount / rowCount) * 100).toFixed(2) : 0}%\n\n`;

        if (missingFieldErrors && missingFieldErrors.length > 0) {
            report += 'MISSING FIELD DETAILS:\n';
            report += '-'.repeat(80) + '\n';

            // Group by missing field type
            const groupedByField = missingFieldErrors.reduce((acc, err) => {
                const field = err.missingField;
                if (!acc[field]) acc[field] = [];
                acc[field].push(err);
                return acc;
            }, {});

            for (const [field, errors] of Object.entries(groupedByField)) {
                report += `\n${field.toUpperCase()} - ${errors.length} occurrence(s):\n`;
                report += '  ' + '-'.repeat(76) + '\n';

                errors.forEach((err, idx) => {
                    report += `  ${idx + 1}. Row #${err.row}\n`;
                    report += `     Reason: ${err.reason}\n\n`;
                });
            }
        } else {
            report += 'MISSING FIELD DETAILS:\n';
            report += '-'.repeat(80) + '\n';
            report += 'No missing fields detected. All rows were valid.\n\n';
        }

        report += '='.repeat(80) + '\n';
        report += 'END OF REPORT\n';
        report += '='.repeat(80) + '\n';

        return report;
    }

    /**
     * Append summary to log file
     */
    async appendSummaryToLog(fileName, results) {
        try {
            const logFilePath = path.join('./logs', 'combined.log');
            const summary = this.generateSummaryReport(fileName, results);

            await fs.appendFile(logFilePath, summary);
            logger.info('Summary report appended to log file', { logFilePath });

            return logFilePath;
        } catch (error) {
            logger.error('Error appending summary to log', { error: error.message });
            throw error;
        }
    }


    /**
     * Create CSV file with remark
     */
    async createRemarkCSV(fileName, results) {
        try {
            const remarkCSVFilePath = path.join('./logs', fileName);
            // const summary = this.generateSummaryReport(fileName, results);

            await fs.writeFile(remarkCSVFilePath, results);
            // logger.info('Summary report appended to log file', { remarkCSVFilePath });

            return remarkCSVFilePath;
        } catch (error) {
            logger.error('Error createRemarkCSV summary to log', { error: error.message });
            throw error;
        }
    }

    /**
     * Process a single orders file
     */
    async processOrdersFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.sourcePath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.sourcePath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // 3. Process rows
            logger.info('Step 3: Inserting rows into orders_stage', {
                fileName,
                rowCount: rows.length
            });
            logger.info('Validating rows (caseid and productid are required)', { fileName });

            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processOrdersRows(rows, fileName);

            logger.info('Stage table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped due to missing caseid/productid`
                    : 'All rows valid'
            });

            // 4. Call merge procedure
            logger.info('Step 4: Calling merge_orders_stage() stored procedure', { fileName });
            await this.dbService.callMergeProcedure();
            logger.info('Merge completed successfully');

            // 5. Move file to processed
            logger.info('Step 5: Moving file to processed folder', { fileName });
            await this.s3Service.moveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;

            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // NEW: 6. Generate and append summary to log
            logger.info('Step 6: Generating summary report', { fileName });
            const logFilePath = await this.appendSummaryToLog(fileName, results);

            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark)
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // NEW: 7. Upload log file to S3
                logger.info('Step 7: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.uploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('✓ File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                skippedReason: errorCount > 0 ? 'Missing caseid or productid' : 'None',
                duration: `${duration}ms`,
                durationSeconds: `${(duration / 1000).toFixed(2)}s`,
                // logFileS3: s3LogKey
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString(),
                // logFileS3: s3LogKey
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('✗ File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process a single file
     * MODIFIED: Added summary generation and S3 upload
     */
    async processLabProductFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.lab_product_sourcepath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.lab_product_sourcepath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processLabProductRows(rows, fileName);

            logger.info('Lab Product table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped`
                    : 'All rows valid'
            });

            // 5. Move file to processed
            logger.info('Step 5: Moving file to processed folder', { fileName });
            await this.s3Service.labProductMoveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;


            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // NEW: 6. Generate and append summary to log
            logger.info('Step 6: Generating summary report', { fileName });
            /**need to update */
            await this.appendSummaryToLog(fileName, results);

            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark)
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // NEW: 7. Upload log file to S3
                logger.info('Step 7: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.labProductUploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('✓ File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                skippedReason: errorCount > 0 ? 'Missing caseid or productid' : 'None',
                duration: `${duration}ms`,
                durationSeconds: `${(duration / 1000).toFixed(2)}s`,
                // logFileS3: s3LogKey
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString(),
                // logFileS3: s3LogKey
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('✗ File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process a single file
     * MODIFIED: Added summary generation and S3 upload
     */
    async processLabPracticeFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.lab_practice_sourcepath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.lab_practice_sourcepath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processLabPracticeRows(rows, fileName);

            logger.info('Dental Practices table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped`
                    : 'All rows valid'
            });

            // 5. Move file to processed
            logger.info('Step 5: Moving file to processed folder', { fileName });
            await this.s3Service.labPracticeMoveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;


            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // NEW: 6. Generate and append summary to log
            logger.info('Step 6: Generating summary report', { fileName });
            /**Need to update */
            await this.appendSummaryToLog(fileName, results);
            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark)
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // NEW: 7. Upload log file to S3
                logger.info('Step 7: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.labPracticeUploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('✓ File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                skippedReason: errorCount > 0 ? 'Missing caseid or productid' : 'None',
                duration: `${duration}ms`,
                durationSeconds: `${(duration / 1000).toFixed(2)}s`,
                // logFileS3: s3LogKey
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString(),
                // logFileS3: s3LogKey
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('✗ File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process a single lab product mapping file
     */
    async processLabProductMappingFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting lab product mapping file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.lab_product_mapping_sourcepath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.lab_product_mapping_sourcepath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // 3. Process rows
            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processLabProductMappingRows(rows, fileName);

            logger.info('Lab Product Mapping table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped`
                    : 'All rows valid'
            });

            // 4. Move file to processed
            logger.info('Step 4: Moving file to processed folder', { fileName });
            await this.s3Service.labProductMappingMoveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;

            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // 5. Generate and append summary to log
            logger.info('Step 5: Generating summary report', { fileName });
            await this.appendSummaryToLog(fileName, results);

            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark);
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // 6. Upload log file to S3
                logger.info('Step 6: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.labProductMappingUploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString()
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process a single lab practice mapping file
     */
    async processLabPracticeMappingFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting lab practice mapping file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.lab_practice_mapping_sourcepath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.lab_practice_mapping_sourcepath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // 3. Process rows
            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processLabPracticeMappingRows(rows, fileName);

            logger.info('Lab Practice Mapping table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped`
                    : 'All rows valid'
            });

            // 4. Move file to processed
            logger.info('Step 4: Moving file to processed folder', { fileName });
            await this.s3Service.labPracticeMappingMoveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;

            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // 5. Generate and append summary to log
            logger.info('Step 5: Generating summary report', { fileName });
            await this.appendSummaryToLog(fileName, results);

            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark);
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // 6. Upload log file to S3
                logger.info('Step 6: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.labPracticeMappingUploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString()
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process all order files in S3
     */
    async processAllOrderFiles() {
        try {
            logger.info('Scanning for order files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.sourcePath
            });

            const files = await this.s3Service.listFiles();

            if (files.length === 0) {
                logger.info('No order files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} order CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processOrdersFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Order files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing order files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process all lab product files in S3
     */
    async processAllLabProductFiles() {
        try {
            logger.info('Scanning for lab product files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.lab_product_sourcepath
            });

            const files = await this.s3Service.listLabProductFiles();

            if (files.length === 0) {
                logger.info('No lab product files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} lab product CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processLabProductFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Lab product files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing lab product files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process all lab practice files in S3
     */
    async processAllLabPracticeFiles() {
        try {
            logger.info('Scanning for lab practice files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.lab_practice_sourcepath
            });

            const files = await this.s3Service.listLabPracticeFiles();

            if (files.length === 0) {
                logger.info('No lab practice files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} lab practice CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processLabPracticeFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Lab practice files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing lab practice files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process all lab product mapping files in S3
     */
    async processAllLabProductMappingFiles() {
        try {
            logger.info('Scanning for lab product mapping files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.lab_product_mapping_sourcepath
            });

            const files = await this.s3Service.listLabProductMappingFiles();

            if (files.length === 0) {
                logger.info('No lab product mapping files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} lab product mapping CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processLabProductMappingFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Lab product mapping files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing lab product mapping files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process all lab practice mapping files in S3
     */
    async processAllLabPracticeMappingFiles() {
        try {
            logger.info('Scanning for lab practice mapping files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.lab_practice_mapping_sourcepath
            });

            const files = await this.s3Service.listLabPracticeMappingFiles();

            if (files.length === 0) {
                logger.info('No lab practice mapping files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} lab practice mapping CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processLabPracticeMappingFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Lab practice mapping files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing lab practice mapping files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process a single dental groups file
     */
    async processDentalGroupsFile(fileName) {
        const startTime = Date.now();
        logger.info('='.repeat(80));
        logger.info('Starting dental groups file processing', {
            fileName,
            startTime: new Date().toISOString()
        });

        try {
            // 1. Check if file exists
            logger.info('Step 1: Checking if file exists in S3', { fileName });
            const exists = await this.s3Service.checkFileExists(fileName, this.s3Service.dental_groups_sourcepath);

            if (!exists) {
                throw new Error(`File not found: ${fileName}`);
            }
            logger.info('File exists in S3', { fileName });

            // 2. Get and parse CSV
            logger.info('Step 2: Retrieving and parsing CSV file', { fileName });
            const stream = await this.s3Service.getFile(fileName, this.s3Service.dental_groups_sourcepath);
            const rows = await this.csvProcessor.parseCSV(stream);
            logger.info('CSV parsed successfully', { rowCount: rows.length, fileName });

            if (rows.length === 0) {
                logger.warn('CSV file is empty, skipping processing', { fileName });
                return {
                    success: true,
                    rowCount: 0,
                    successCount: 0,
                    errorCount: 0,
                    missingFieldErrors: [],
                    skipped: true
                };
            }

            // 3. Process rows
            const { successCount, errorCount, missingFieldErrors, csvRemark } = await this.csvProcessor.processDentalGroupsRows(rows, fileName);

            logger.info('Dental Groups table populated', {
                successCount,
                errorCount,
                message: errorCount > 0
                    ? `${errorCount} rows skipped`
                    : 'All rows valid'
            });

            // 4. Move file to processed
            logger.info('Step 4: Moving file to processed folder', { fileName });
            await this.s3Service.dentalGroupsMoveToProcessed(fileName);
            logger.info('File moved to processed folder');

            const duration = Date.now() - startTime;

            const results = {
                rowCount: rows.length,
                successCount,
                errorCount,
                missingFieldErrors,
                duration
            };

            // 5. Generate and append summary to log
            logger.info('Step 5: Generating summary report', { fileName });
            await this.appendSummaryToLog(fileName, results);

            if (errorCount > 0) {
                const updatesCSVRemark = objectToCSV(csvRemark);
                const remarkCSVFilePath = await this.createRemarkCSV(fileName, updatesCSVRemark);

                // 6. Upload log file to S3
                logger.info('Step 6: Uploading log file to S3', { fileName });
                const s3LogKey = await this.s3Service.dentalGroupsUploadLogFile(
                    remarkCSVFilePath,
                    `${fileName}`
                );
                logger.info('Log file uploaded successfully', { s3LogKey });
            }

            logger.info('File processing completed successfully', {
                fileName,
                totalRows: rows.length,
                validRows: successCount,
                invalidRows: errorCount,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));

            return {
                success: true,
                rowCount: rows.length,
                successCount,
                errorCount,
                skippedCount: errorCount,
                missingFieldErrors,
                duration,
                processedAt: new Date().toISOString()
            };

        } catch (error) {
            const duration = Date.now() - startTime;
            logger.error('File processing failed', {
                fileName,
                error: error.message,
                stack: error.stack,
                duration: `${duration}ms`
            });
            logger.info('='.repeat(80));
            throw error;
        }
    }

    /**
     * Process all dental groups files in S3
     */
    async processAllDentalGroupsFiles() {
        try {
            logger.info('Scanning for dental groups files in S3', {
                bucket: this.s3Service.bucket,
                path: this.s3Service.dental_groups_sourcepath
            });

            const files = await this.s3Service.listDentalGroupsFiles();

            if (files.length === 0) {
                logger.info('No dental groups files found to process');
                return { processed: 0, successful: 0, failed: 0, results: [] };
            }

            logger.info(`Found ${files.length} dental groups CSV file(s) to process`, { files });

            const results = [];
            let successful = 0;
            let failed = 0;

            for (const file of files) {
                try {
                    const result = await this.processDentalGroupsFile(file);
                    results.push({ file, ...result });
                    if (result.success) successful++;
                    else failed++;
                } catch (error) {
                    results.push({ file, success: false, error: error.message });
                    failed++;
                }
            }

            logger.info('Dental groups files processed', { total: files.length, successful, failed });
            return { processed: files.length, successful, failed, results };
        } catch (error) {
            logger.error('Error processing dental groups files', { error: error.message });
            throw error;
        }
    }

    /**
     * Process all files from all pipelines
     */
    async processAllFiles() {
        const ordersResult = await this.processAllOrderFiles();
        const labProductResult = await this.processAllLabProductFiles();
        const labPracticeResult = await this.processAllLabPracticeFiles();
        const labProductMappingResult = await this.processAllLabProductMappingFiles();
        const labPracticeMappingResult = await this.processAllLabPracticeMappingFiles();
        const dentalGroupsResult = await this.processAllDentalGroupsFiles();

        const totalProcessed = ordersResult.processed + labProductResult.processed + labPracticeResult.processed + labProductMappingResult.processed + labPracticeMappingResult.processed + dentalGroupsResult.processed;
        const totalSuccessful = ordersResult.successful + labProductResult.successful + labPracticeResult.successful + labProductMappingResult.successful + labPracticeMappingResult.successful + dentalGroupsResult.successful;
        const totalFailed = ordersResult.failed + labProductResult.failed + labPracticeResult.failed + labProductMappingResult.failed + labPracticeMappingResult.failed + dentalGroupsResult.failed;

        logger.info('All pipelines processed', {
            totalFiles: totalProcessed,
            successful: totalSuccessful,
            failed: totalFailed
        });

        return {
            processed: totalProcessed,
            successful: totalSuccessful,
            failed: totalFailed,
            orders: ordersResult,
            labProducts: labProductResult,
            labPractices: labPracticeResult,
            labProductMappings: labProductMappingResult,
            labPracticeMappings: labPracticeMappingResult,
            dentalGroups: dentalGroupsResult
        };
    }
}

module.exports = ETLProcessor;