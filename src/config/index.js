require('dotenv').config();

/**
 * Get required environment variable or throw error
 */
function getRequiredEnv(name) {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }
    return value;
}

/**
 * Get optional environment variable with default
 */
function getOptionalEnv(name, defaultValue) {
    return process.env[name] || defaultValue;
}

/**
 * Build pipeline paths from base path
 */
function buildPipelinePaths(basePath) {
    if (!basePath) {
        return { sourcePath: null, processedPath: null, logsPath: null };
    }
    return {
        sourcePath: `${basePath}/`,
        processedPath: `${basePath}/processed/`,
        logsPath: `${basePath}/logs/`
    };
}

// Build paths for each pipeline
const ordersPaths = buildPipelinePaths(process.env.SOURCEPATH);
const labProductPaths = buildPipelinePaths(process.env.LAB_PRODUCT_SOURCEPATH);
const labPracticePaths = buildPipelinePaths(process.env.LAB_PRACTICE_SOURCEPATH);
const labProductMappingPaths = buildPipelinePaths(process.env.LAB_PRODUCT_MAPPING_SOURCEPATH);
const labPracticeMappingPaths = buildPipelinePaths(process.env.LAB_PRACTICE_MAPPING_SOURCEPATH);
const dentalGroupsPaths = buildPipelinePaths(process.env.DENTAL_GROUPS_SOURCEPATH);

module.exports = {
    aws: {
        region: getOptionalEnv('AWS_REGION', 'us-east-1'),
        bucket: getOptionalEnv('S3_BUCKET', 'dev-incisive-data-csv'),

        // Orders pipeline paths
        sourcePath: ordersPaths.sourcePath,
        processedPath: ordersPaths.processedPath,
        logsPath: ordersPaths.logsPath,

        // Lab product pipeline paths
        lab_product_sourcepath: labProductPaths.sourcePath,
        lab_product_processedPath: labProductPaths.processedPath,
        lab_product_logsPath: labProductPaths.logsPath,

        // Lab practice pipeline paths
        lab_practice_sourcepath: labPracticePaths.sourcePath,
        lab_practice_processedPath: labPracticePaths.processedPath,
        lab_practice_logsPath: labPracticePaths.logsPath,

        // Lab product mapping pipeline paths
        lab_product_mapping_sourcepath: labProductMappingPaths.sourcePath,
        lab_product_mapping_processedPath: labProductMappingPaths.processedPath,
        lab_product_mapping_logsPath: labProductMappingPaths.logsPath,

        // Lab practice mapping pipeline paths
        lab_practice_mapping_sourcepath: labPracticeMappingPaths.sourcePath,
        lab_practice_mapping_processedPath: labPracticeMappingPaths.processedPath,
        lab_practice_mapping_logsPath: labPracticeMappingPaths.logsPath,

        // Dental groups pipeline paths
        dental_groups_sourcepath: dentalGroupsPaths.sourcePath,
        dental_groups_processedPath: dentalGroupsPaths.processedPath,
        dental_groups_logsPath: dentalGroupsPaths.logsPath,

        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
    },
    db: {
        host: getOptionalEnv('DB_HOST', 'localhost'),
        port: parseInt(getOptionalEnv('DB_PORT', '5432'), 10),
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: getOptionalEnv('DB_NAME', 'postgres'),
        options: '-c search_path=etl,public',
        max: parseInt(getOptionalEnv('DB_POOL_MAX', '10'), 10),
        idleTimeoutMillis: parseInt(getOptionalEnv('DB_IDLE_TIMEOUT', '30000'), 10),
        connectionTimeoutMillis: parseInt(getOptionalEnv('DB_CONNECT_TIMEOUT', '2000'), 10),
        ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: process.env.DB_SSL_REJECT_UNAUTHORIZED !== 'false' } : false
    },
    processing: {
        batchSize: parseInt(getOptionalEnv('BATCH_SIZE', '100'), 10)
    }
};