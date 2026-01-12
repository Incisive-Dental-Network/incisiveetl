require('dotenv').config();

module.exports = {
    aws: {
        region: process.env.AWS_REGION || 'us-east-1',
        bucket: process.env.S3_BUCKET || 'dev-incisive-data-csv',
        sourcePath: `${process.env.SOURCEPATH}/`,
        processedPath: `${process.env.SOURCEPATH}/processed/`,
        logsPath: `${process.env.SOURCEPATH}/logs/`,
        credentials: {
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
        }
    },
    db: {
        host: process.env.DB_HOST || 'localhost',
        port: parseInt(process.env.DB_PORT) || 5432,
        user: process.env.DB_USER || 'incisive_dev_glue_user',
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME || 'postgres',
        options: '-c search_path=etl,public',
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 2000,
        ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: false } : false
    },
    processing: {
        batchSize: parseInt(process.env.BATCH_SIZE) || 100
    }
};