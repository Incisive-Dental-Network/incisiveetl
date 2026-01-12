const S3Service = require('./src/services/s3service');
const DBService = require('./src/services/dbservice');
const CSVProcessor = require('./src/services/csvProcessor');
const ETLProcessor = require('./src/etl/processor');
const logger = require('./src/utils/logger');

// Initialize services
const s3Service = new S3Service();
const dbService = new DBService();
const csvProcessor = new CSVProcessor(dbService);
const etlProcessor = new ETLProcessor(s3Service, dbService, csvProcessor);

/**
 * Graceful shutdown
 */
async function shutdown() {
    logger.info('Shutting down gracefully...');
    await dbService.close();
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Export for use as module
module.exports = {
    processFile: (fileName) => etlProcessor.processFile(fileName),
    processAllFiles: () => etlProcessor.processAllFiles(),
    s3Service,
    dbService,
    csvProcessor,
    etlProcessor
};

// Run if executed directly
if (require.main === module) {
    const fileName = process.argv[2];

    (async () => {
        try {
            if (fileName) {
                await etlProcessor.processFile(fileName);
                await shutdown();
            } else {
                await etlProcessor.processAllFiles();
                await shutdown();
            }
        } catch (error) {
            logger.error('Fatal error', { error: error.message });
            process.exit(1);
        }
    })();
}