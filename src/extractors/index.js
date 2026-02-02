/**
 * Extractor Registry
 * ==================
 * Central registry for data extraction modules.
 *
 * Currently supported extractors:
 * - salesforce: Extract data from Salesforce using JWT authentication
 * - magictouch: Extract orders from MagicTouch API
 *
 * Usage:
 * const { SalesforceExtractor, MagicTouchExtractor } = require('./extractors');
 * const sfExtractor = new SalesforceExtractor(config, s3Handler);
 * const mtExtractor = new MagicTouchExtractor(config, s3Handler);
 */

const SalesforceExtractor = require('./salesforce');
const MagicTouchExtractor = require('./magictouch');

module.exports = {
    SalesforceExtractor,
    MagicTouchExtractor
};
