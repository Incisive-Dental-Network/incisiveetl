/**
 * MagicTouch Client
 * =================
 * Reusable MagicTouch API client with token authentication.
 *
 * Features:
 * - Token-based authentication
 * - Cases and Customers query endpoints
 * - Automatic pagination handling
 *
 * Usage:
 * const client = new MagicTouchClient(config);
 * await client.authenticate();
 * const cases = await client.fetchCases('Incisive');
 */

const https = require('https');
const logger = require('../../utils/logger');

class MagicTouchClient {
    /**
     * Initialize MagicTouch client
     * @param {Object} config - MagicTouch configuration
     * @param {string} config.baseUrl - MagicTouch API base URL
     * @param {string} config.userID - MagicTouch user ID
     * @param {string} config.password - MagicTouch password
     * @param {string} config.exportMode - 'INC' for incremental or 'FULL' for all
     */
    constructor(config) {
        this.baseUrl = config.baseUrl;
        this.userID = config.userID;
        this.password = config.password;
        this.exportMode = (config.exportMode || 'INC').toUpperCase();
        this.token = null;
        this.pageSize = 100;
    }

    /**
     * Make HTTP request
     * @param {string} url - Full URL
     * @param {Object} options - Request options
     * @returns {Promise<Object>} JSON response
     */
    makeRequest(url, options = {}) {
        return new Promise((resolve, reject) => {
            const urlObj = new URL(url);
            const reqOptions = {
                hostname: urlObj.hostname,
                path: urlObj.pathname + urlObj.search,
                method: options.method || 'GET',
                headers: options.headers || {}
            };

            const req = https.request(reqOptions, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        if (res.statusCode >= 200 && res.statusCode < 300) {
                            resolve(json);
                        } else {
                            reject({ statusCode: res.statusCode, body: json });
                        }
                    } catch (e) {
                        reject({ statusCode: res.statusCode, body: data });
                    }
                });
            });

            req.on('error', reject);
            if (options.body) req.write(options.body);
            req.end();
        });
    }

    /**
     * Authenticate to MagicTouch API
     * @returns {Promise<void>}
     */
    async authenticate() {
        try {
            logger.info('MagicTouchClient: Authenticating', { baseUrl: this.baseUrl });

            const response = await this.makeRequest(`${this.baseUrl}/api/Authentication/authenticate`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    userID: this.userID,
                    password: this.password
                })
            });

            this.token = response.token;
            logger.info('MagicTouchClient: Authentication successful');

        } catch (error) {
            const errorMessage = error.body?.message || error.message || 'Unknown error';
            logger.error('MagicTouchClient: Authentication failed', {
                error: errorMessage,
                statusCode: error.statusCode
            });
            throw new Error(`MagicTouch authentication failed: ${errorMessage}`);
        }
    }

    /**
     * Get date filter based on export mode
     * @returns {string|null} Date filter or null for FULL mode
     */
    getDateFilter() {
        if (this.exportMode === 'FULL') {
            return null;
        }

        // INC mode: get cases modified in the last 7 days
        const oneWeekAgo = new Date();
        oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

        const year = oneWeekAgo.getFullYear();
        const month = oneWeekAgo.getMonth() + 1;
        const day = oneWeekAgo.getDate();

        return `modifyDate >= DateTime(${year},${month},${day})`;
    }

    /**
     * Fetch cases from MagicTouch with pagination
     * @param {string} customerType - Customer type filter (e.g., 'Incisive')
     * @returns {Promise<Object[]>} Array of case records
     */
    async fetchCases(customerType) {
        const dateFilter = this.getDateFilter();

        if (this.exportMode === 'INC') {
            logger.info('MagicTouchClient: Mode INCREMENTAL (last 7 days)', { dateFilter });
        } else {
            logger.info('MagicTouchClient: Mode FULL (all cases)');
        }

        const allCases = [];
        let page = 1;
        let hasMore = true;
        let totalCount = null;

        while (hasMore) {
            // Build query
            let queryStr = `customer.type == "${customerType}"`;
            if (dateFilter) {
                queryStr += ` && ${dateFilter}`;
            }
            const query = encodeURIComponent(queryStr);

            const response = await this.makeRequest(
                `${this.baseUrl}/api/Cases/QueryCases?query=${query}&page=${page}&pageSize=${this.pageSize}`,
                {
                    headers: {
                        'Authorization': `Bearer ${this.token}`,
                        'Content-Type': 'application/json'
                    }
                }
            );

            if (totalCount === null && response.totalCount) {
                totalCount = response.totalCount;
                logger.info('MagicTouchClient: Total cases to fetch', { totalCount });
            }

            const items = response.results || [];
            allCases.push(...items);

            logger.info('MagicTouchClient: Fetched page', {
                page,
                pageSize: items.length,
                totalSoFar: allCases.length
            });

            if (items.length < this.pageSize) {
                hasMore = false;
            } else {
                page++;
            }
        }

        logger.info('MagicTouchClient: Cases fetch complete', { totalCases: allCases.length });
        return allCases;
    }

    /**
     * Fetch customers for phone number lookup
     * @param {string} customerType - Customer type filter
     * @returns {Promise<Object>} Map of customerID -> { customerName, officePhone }
     */
    async fetchCustomers(customerType) {
        logger.info('MagicTouchClient: Fetching customers for phone lookup');

        const customers = {};
        let page = 1;
        let hasMore = true;

        while (hasMore) {
            const query = encodeURIComponent(`type == "${customerType}"`);
            const response = await this.makeRequest(
                `${this.baseUrl}/api/Customers/QueryCustomers?query=${query}&page=${page}&pageSize=${this.pageSize}`,
                {
                    headers: {
                        'Authorization': `Bearer ${this.token}`,
                        'Content-Type': 'application/json'
                    }
                }
            );

            const items = response.results || [];

            for (const c of items) {
                customers[c.customerID] = {
                    customerName: c.customerName || c.customerID,
                    officePhone: c.officePhone || ''
                };
            }

            if (items.length < this.pageSize) {
                hasMore = false;
            } else {
                page++;
            }
        }

        logger.info('MagicTouchClient: Customers loaded', { count: Object.keys(customers).length });
        return customers;
    }
}

module.exports = MagicTouchClient;
