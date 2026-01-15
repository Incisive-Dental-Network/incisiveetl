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
    processAllOrderFiles: () => etlProcessor.processAllOrderFiles(),
    processAllLabProductFiles: () => etlProcessor.processAllLabProductFiles(),
    processAllLabPracticeFiles: () => etlProcessor.processAllLabPracticeFiles(),
    processAllLabProductMappingFiles: () => etlProcessor.processAllLabProductMappingFiles(),
    processAllLabPracticeMappingFiles: () => etlProcessor.processAllLabPracticeMappingFiles(),
    processAllDentalGroupsFiles: () => etlProcessor.processAllDentalGroupsFiles(),
    processAllFiles: () => etlProcessor.processAllFiles(),
    s3Service,
    dbService,
    csvProcessor,
    etlProcessor
};

// Run if executed directly
if (require.main === module) {
    const command = process.argv[2];

    (async () => {
        try {
            switch (command) {
                case 'orders':
                    await etlProcessor.processAllOrderFiles();
                    break;
                case 'products':
                    await etlProcessor.processAllLabProductFiles();
                    break;
                case 'practices':
                    await etlProcessor.processAllLabPracticeFiles();
                    break;
                case 'mappings':
                    await etlProcessor.processAllLabProductMappingFiles();
                    break;
                case 'practice-mappings':
                    await etlProcessor.processAllLabPracticeMappingFiles();
                    break;
                case 'dental-groups':
                    await etlProcessor.processAllDentalGroupsFiles();
                    break;
                case 'all':
                    await etlProcessor.processAllFiles();
                    break;
                default:
                    // If a filename is provided, process as single order file
                    if (command) {
                        await etlProcessor.processOrdersFile(command);
                    } else {
                        // No args: process all pipelines
                        await etlProcessor.processAllFiles();
                    }
            }
            await shutdown();
        } catch (error) {
            logger.error('Fatal error', { error: error.message });
            process.exit(1);
        }
    })();
}