const { Pool } = require('pg');
const config = require('../config');
const logger = require('../utils/logger');
const { generateRowHash } = require('../utils/hash');

class DBService {
    constructor() {
        this.pool = new Pool(config.db);

        this.pool.on('error', (err) => {
            logger.error('Unexpected database pool error', { error: err.message });
        });
    }

    /**
     * Truncate staging table
     */
    async truncateStage(client) {
        await client.query('TRUNCATE TABLE orders_stage');
        logger.info('Staging table truncated');
    }

    /**
     * Insert single row into staging table
     */
    async insertStageRow(client, row, fileName) {
        // Validate required fields
        const caseid = row.caseid ? parseInt(row.caseid) : null;
        const productid = row.productid || null;

        if (!caseid || !productid) {
            return {
                success: false,
                reason: !caseid ? 'Missing caseid' : 'Missing productid',
                caseid: row.caseid,
                productid: row.productid
            };
        }

        const rowHash = generateRowHash(row);
        const sourceFileKey = config.aws.sourcePath + fileName;

        const sql = `
      INSERT INTO orders_stage (
        submissiondate, shippingdate, casedate, caseid, productid,
        productdescription, quantity, productprice, patientname,
        customerid, customername, address, phonenumber, casestatus,
        holdreason, estimatecompletedate, requestedreturndate,
        trackingnumber, estimatedshipdate, holddate, deliverystatus,
        notes, onhold, shade, mold, doctorpreferences,
        productpreferences, comments, casetotal,
        source_file_key, row_hash
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31
      )
    `;

        const values = [
            row.submissiondate || null,
            row.shippingdate || null,
            row.casedate || null,
            caseid,
            productid,
            row.productdescription || null,
            row.quantity ? parseInt(row.quantity) : null,
            row.productprice || null,
            row.patientname || null,
            row.customerid || null,
            row.customername || null,
            row.address || null,
            row.phonenumber || null,
            row.casestatus || null,
            row.holdreason || null,
            row.estimatecompletedate || null,
            row.requestedreturndate || null,
            row.trackingnumber || null,
            row.estimatedshipdate || null,
            row.holddate || null,
            row.deliverystatus || null,
            row.notes || null,
            row.onhold || null,
            row.shade || null,
            row.mold || null,
            row.doctorpreferences || null,
            row.productpreferences || null,
            row.comments || null,
            row.casetotal || null,
            sourceFileKey,
            rowHash
        ];

        await client.query(sql, values);
        return { success: true, caseid, productid };
    }

    /**
     * Call merge stored procedure
     */
    async callMergeProcedure() {
        const client = await this.pool.connect();

        try {
            logger.info('Calling merge_orders_stage() stored procedure');
            await client.query('CALL merge_orders_stage()');
            logger.info('Stored procedure executed successfully');
        } catch (error) {
            logger.error('Error calling stored procedure', {
                error: error.message,
                stack: error.stack
            });
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * 
     */
    async insertProductCatlog(client, row) {
        const sql = `
            INSERT INTO incisive_product_catalog (
            incisive_id, incisive_name, category, sub_category
            ) VALUES (
            $1, $2, $3, $4
            )
            ON CONFLICT (incisive_id) DO NOTHING
        `;

        const values = [
            row.incisive_id,
            row.incisive_name,
            row.category,
            row.sub_category,
        ];

        try {
            await client.query(sql, values);
            return { success: true };

        } catch (error) {
            return {
                success: false,
                errorRow: {
                    row,
                    error_code: error.code,
                    error_message: error.message
                }
            }
        }
    }

    /**
     * 
     */
    async insertDentalPractices(client, row) {
        const sql = `
            INSERT INTO dental_practices (
                practice_id,
                dental_group_id,
                dental_group_name,
                address,
                address_2,
                city,
                state,
                zip,
                phone,
                clinical_email,
                billing_email,
                incisive_email,
                preferred_contact_method,
                fee_schedule,
                status
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15
            )
                ON CONFLICT (practice_id) DO NOTHING
            `;

        const values = [
            row.practice_id,
            row.dental_group_id,
            row.dental_group_name,
            row.address,
            row.address_2,
            row.city,
            row.state,
            row.zip,
            row.phone,
            row.clinical_email,
            row.billing_email,
            row.incisive_email,
            row.preferred_contact_method,
            row.fee_schedule,
            row.status
        ];


        try {
            await client.query(sql, values);
            return { success: true };

        } catch (error) {
            return {
                success: false,
                errorRow: {
                    row,
                    error_code: error.code,
                    error_message: error.message
                }
            }
        }
    }

    /**
     * Close database pool
     */
    async close() {
        await this.pool.end();
        logger.info('Database pool closed');
    }
}

module.exports = DBService;