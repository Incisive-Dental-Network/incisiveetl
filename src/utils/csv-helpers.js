/**
 * Normalize CSV headers to lowercase and remove spaces/special chars
 * @param {Object} row - Raw CSV row object
 * @returns {Object} Normalized row object
 */
function normalizeCSVRow(row) {
    const normalized = {};

    for (const [key, value] of Object.entries(row)) {
        // Convert to lowercase, trim spaces, remove special characters
        const normalizedKey = key
            .toLowerCase()
            .trim()
            .replace(/[^a-z0-9]/g, ''); // Remove non-alphanumeric

        normalized[normalizedKey] = value;
    }

    return normalized;
}

/**
 * Map normalized CSV row to database schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToSchema(row) {
    return {
        submissiondate: row.submissiondate || null,
        shippingdate: row.shippingdate || null,
        casedate: row.casedate || null,
        caseid: row.caseid || null,
        productid: row.productid || null,
        productdescription: row.productdescription || null,
        quantity: row.quantity ? parseInt(row.quantity, 10) : null,
        productprice: row.productprice || null,
        patientname: row.patientname || null,
        customerid: row.customerid || null,
        customername: row.customername || null,
        address: row.address || null,
        phonenumber: row.phonenumber || null,
        casestatus: row.casestatus || null,
        holdreason: row.holdreason || null,
        estimatecompletedate: row.estimatecompletedate || null,
        requestedreturndate: row.requestedreturndate || null,
        trackingnumber: row.trackingnumber || null,
        estimatedshipdate: row.estimatedshipdate || null,
        holddate: row.holddate || null,
        deliverystatus: row.deliverystatus || null,
        notes: row.notes || null,
        onhold: row.onhold || null,
        shade: row.shade || null,
        mold: row.mold || null,
        doctorpreferences: row.doctorpreferences || null,
        productpreferences: row.productpreferences || null,
        comments: row.comments || null,
        casetotal: row.casetotal || null
    };
}


/**
 * Map normalized CSV row to database schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToLabProductSchema(row) {
    const incisiveId = row.incisiveid ? Number(row.incisiveid) : null;
    return {
        incisive_id: Number.isNaN(incisiveId) ? null : incisiveId,
        incisive_name: row.incisivename || null,
        category: row.category || null,
        sub_category: row.subcategory || null
    };
}

/**
 * Map normalized CSV row to database schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToLabPracticeSchema(row) {
    const practiceId = row.practiceid ? Number(row.practiceid) : null;
    const dentalGroupId = row.dentalgroupid ? Number(row.dentalgroupid) : null;
    return {
        practice_id: Number.isNaN(practiceId) ? null : practiceId,
        dental_group_id: Number.isNaN(dentalGroupId) ? null : dentalGroupId,
        dental_group_name: row.dentalgroupname || null,
        address: row.address || null,
        address_2: row.address2 || null,
        city: row.city || null,
        state: row.state || null,
        zip: row.zip || null,
        phone: row.phone || null,
        clinical_email: row.clinicalemail || null,
        billing_email: row.billingemail || null,
        incisive_email: row.incisiveemail || null,
        preferred_contact_method: row.preferredcontactmethod || null,
        fee_schedule: row.feeschedule || null,
        status: row.status || null
    };
}

/**
 * Map normalized CSV row to lab_product_mapping schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToLabProductMappingSchema(row) {
    const labId = row.labid ? Number(row.labid) : null;
    const incisiveProductId = row.incisiveproductid ? Number(row.incisiveproductid) : null;
    return {
        lab_id: Number.isNaN(labId) ? null : labId,
        lab_product_id: row.labproductid || null,
        incisive_product_id: Number.isNaN(incisiveProductId) ? null : incisiveProductId
    };
}

/**
 * Map normalized CSV row to lab_practice_mapping schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToLabPracticeMappingSchema(row) {
    const labId = row.labid ? Number(row.labid) : null;
    const practiceId = row.practiceid ? Number(row.practiceid) : null;
    return {
        lab_id: Number.isNaN(labId) ? null : labId,
        practice_id: Number.isNaN(practiceId) ? null : practiceId,
        lab_practice_id: row.labpracticeid || null
    };
}

/**
 * Map normalized CSV row to dental_groups schema
 * @param {Object} row - Normalized CSV row
 * @returns {Object} Mapped row for database insertion
 */
function mapCSVToDentalGroupsSchema(row) {
    const dentalGroupId = row.dentalgroupid ? Number(row.dentalgroupid) : null;
    // Handle boolean for centralized_billing
    const centralizedBilling = row.centralizedbilling !== undefined && row.centralizedbilling !== null
        ? (row.centralizedbilling === 'true' || row.centralizedbilling === 'TRUE' || row.centralizedbilling === '1' || row.centralizedbilling === true)
        : null;
    return {
        dental_group_id: Number.isNaN(dentalGroupId) ? null : dentalGroupId,
        name: row.name || null,
        address: row.address || null,
        address_2: row.address2 || null,
        city: row.city || null,
        state: row.state || null,
        zip: row.zip || null,
        account_type: row.accounttype || null,
        centralized_billing: centralizedBilling,
        sales_channel: row.saleschannel || null,
        sales_rep: row.salesrep || null
    };
}

/**
 * Debug helper to log CSV headers
 * @param {Object} row - First CSV row
 */
function logCSVHeaders(row) {
    console.log('\n=== CSV Headers Found ===');
    Object.keys(row).forEach((key, index) => {
        console.log(`${index + 1}. "${key}" (length: ${key.length})`);
    });
    console.log('========================\n');
}

function objectToCSV(data) {
    if (!data.length) return '';

    const headers = Object.keys(data[0]).join(',');

    const rows = data.map(row =>
        Object.values(row)
            .map(v => `"${String(v ?? '')
                .replace(/\r?\n/g, ' ')
                .replace(/"/g, '""')}"`)
            .join(',')
    );

    return [headers, ...rows].join('\n');
}

module.exports = {
    normalizeCSVRow,
    mapCSVToSchema,
    mapCSVToLabProductSchema,
    mapCSVToLabPracticeSchema,
    mapCSVToLabProductMappingSchema,
    mapCSVToLabPracticeMappingSchema,
    mapCSVToDentalGroupsSchema,
    logCSVHeaders,
    objectToCSV
};