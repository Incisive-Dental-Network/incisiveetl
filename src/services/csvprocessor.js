const csv = require('csv-parser');
const { Readable } = require('stream');
const logger = require('../utils/logger');
const config = require('../config');
const { normalizeCSVRow, mapCSVToSchema, mapCSVToLabProductSchema, mapCSVToLabPracticeSchema, mapCSVToLabProductMappingSchema, mapCSVToLabPracticeMappingSchema, mapCSVToDentalGroupsSchema } = require('../utils/csv-helpers');

class CSVprocessor {
  constructor(dbService) {
    this.dbService = dbService;
    this.batchSize = config.processing.batchSize;
  }

  /**
   * Parse CSV stream into rows
   */
  async parseCSV(stream) {
    return new Promise((resolve, reject) => {
      const rows = [];
      const readable = Readable.from(stream);
      readable
        .pipe(csv())
        .on('data', (row) => rows.push(row))
        .on('end', () => resolve(rows))
        .on('error', reject);
    });
  }

  /**
   * Generic row processor - handles all pipeline types
   */
  async _processRows(rows, fileName, options) {
    const { requiredFields, mapFunction, insertFunction, shouldTruncate = false } = options;

    const client = await this.dbService.pool.connect();
    let successCount = 0;
    let errorCount = 0;
    const missingFieldErrors = [];
    const csvRemark = [];

    try {
      await client.query('BEGIN');

      if (shouldTruncate) {
        await this.dbService.truncateStage(client);
      }

      for (let i = 0; i < rows.length; i += this.batchSize) {
        const batch = rows.slice(i, i + this.batchSize);

        for (let j = 0; j < batch.length; j++) {
          const row = batch[j];
          const rowNumber = i + j + 1;

          const normalizedRow = normalizeCSVRow(row);
          const mappedRow = mapFunction(normalizedRow);

          try {
            const missingFields = requiredFields.filter(
              key => mappedRow[key] === null || mappedRow[key] === undefined || mappedRow[key] === ''
            );

            if (missingFields.length > 0) {
              errorCount++;
              const errorDetail = {
                ...row,
                reason: `Missing required fields: ${missingFields.join(', ')}`,
                missingFields
              };
              missingFieldErrors.push(errorDetail);
              csvRemark.push(errorDetail);

              logger.error('Skipping row with missing fields', {
                rowNumber,
                reason: errorDetail.reason,
                fileName
              });
            } else {
              const result = await insertFunction(client, mappedRow, fileName);

              if (result.success) {
                csvRemark.push(mappedRow);
                successCount++;
                logger.info('Row inserted successfully', { fileName, rowNumber });
              } else {
                errorCount++;
                const errorDetail = {
                  ...row,
                  reason: result?.errorRow?.error_message || result?.reason || 'Insert failed',
                  missingFields: []
                };
                csvRemark.push(errorDetail);
                missingFieldErrors.push(errorDetail);
                logger.error('Error inserting row', { fileName, rowNumber, reason: errorDetail.reason });
              }
            }
          } catch (error) {
            errorCount++;
            const errorDetail = {
              ...row,
              reason: error.message,
              missingFields: []
            };
            missingFieldErrors.push(errorDetail);
            csvRemark.push(errorDetail);

            logger.error('Error inserting row', {
              rowNumber,
              error: error.message,
              fileName
            });
          }
        }

        logger.info('Batch processed', {
          batchStart: i + 1,
          batchEnd: Math.min(i + this.batchSize, rows.length),
          total: rows.length,
          successCount,
          errorCount
        });
      }

      await client.query('COMMIT');
      logger.info('Transaction committed', {
        fileName,
        totalRows: rows.length,
        successCount,
        errorCount
      });

    } catch (error) {
      await client.query('ROLLBACK');
      logger.error('Transaction rolled back', { error: error.message, fileName });
      throw error;
    } finally {
      client.release();
    }

    return { successCount, errorCount, missingFieldErrors, csvRemark };
  }

  /**
   * Process order rows
   */
  async processOrdersRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['submissiondate', 'casedate', 'caseid', 'productid', 'quantity', 'customerid'],
      mapFunction: mapCSVToSchema,
      insertFunction: (client, row, file) => this.dbService.insertStageRow(client, row, file),
      shouldTruncate: true
    });
  }

  /**
   * Process lab product rows
   */
  async processLabProductRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['incisive_id', 'incisive_name', 'category'],
      mapFunction: mapCSVToLabProductSchema,
      insertFunction: (client, row) => this.dbService.insertProductCatalog(client, row)
    });
  }

  /**
   * Process lab practice rows
   */
  async processLabPracticeRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['practice_id', 'dental_group_id'],
      mapFunction: mapCSVToLabPracticeSchema,
      insertFunction: (client, row) => this.dbService.insertDentalPractices(client, row)
    });
  }

  /**
   * Process lab product mapping rows
   */
  async processLabProductMappingRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['lab_id', 'lab_product_id', 'incisive_product_id'],
      mapFunction: mapCSVToLabProductMappingSchema,
      insertFunction: (client, row) => this.dbService.insertLabProductMapping(client, row)
    });
  }

  /**
   * Process lab practice mapping rows
   */
  async processLabPracticeMappingRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['lab_id', 'practice_id', 'lab_practice_id'],
      mapFunction: mapCSVToLabPracticeMappingSchema,
      insertFunction: (client, row) => this.dbService.insertLabPracticeMapping(client, row)
    });
  }

  /**
   * Process dental groups rows
   */
  async processDentalGroupsRows(rows, fileName) {
    return this._processRows(rows, fileName, {
      requiredFields: ['dental_group_id'],
      mapFunction: mapCSVToDentalGroupsSchema,
      insertFunction: (client, row) => this.dbService.insertDentalGroup(client, row)
    });
  }
}

module.exports = CSVprocessor;