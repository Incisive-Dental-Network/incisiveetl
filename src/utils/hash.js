const crypto = require('crypto');

/**
 * Generate MD5 hash for row deduplication
 */
function generateRowHash(row) {
  const rowString = JSON.stringify(row, Object.keys(row).sort());
  return crypto.createHash('md5').update(rowString).digest('hex');
}

module.exports = { generateRowHash };