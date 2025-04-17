// setup-erpnext-webhook.js - Script to help set up ERPNext webhooks
const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Replace these with your ERPNext instance details
const ERPNEXT_URL = process.env.ERPNEXT_URL || 'http://localhost:8000';
const API_KEY = process.env.ERPNEXT_API_KEY || 'your-api-key';
const API_SECRET = process.env.ERPNEXT_API_SECRET || 'your-api-secret';

// Middleware webhook URL
const MIDDLEWARE_URL = process.env.MIDDLEWARE_URL || 'http://localhost:3000/webhook/erp-document';

// Document types to track
const DOCUMENT_TYPES = [
  'Sales Invoice',
  'Purchase Invoice',
  'Payment Entry',
  'Journal Entry',
  'Stock Entry',
  'Delivery Note',
  'Purchase Receipt'
];

// Events to track
const EVENTS = [
  'on_submit',
  'on_cancel',
  'on_update',
  'after_insert'
];

// Function to create a webhook in ERPNext
async function createWebhook(doctype, event) {
  try {
    console.log(`Creating webhook for ${doctype} - ${event}...`);

    const response = await axios.post(
      `${ERPNEXT_URL}/api/resource/Webhook`,
      {
        webhook_name: `Blockchain - ${doctype} ${event}`,
        webhook_doctype: doctype,
        webhook_docevent: event,
        enabled: 1,
        request_url: MIDDLEWARE_URL,
        request_method: 'POST',
        request_structure: 'Form URL-Encoded',
        webhook_headers: [
          {
            key: 'Content-Type',
            value: 'application/json'
          }
        ],
        webhook_data: [
          {
            fieldname: 'doctype',
            key: 'doctype',
            value: doctype
          },
          {
            fieldname: 'name',
            key: 'name',
            value: 'doc.name'
          },
          {
            fieldname: 'event',
            key: 'event',
            value: event
          },
          {
            fieldname: 'data',
            key: 'data',
            value: 'doc'
          }
        ]
      },
      {
        headers: {
          'Authorization': `token ${API_KEY}:${API_SECRET}`
        }
      }
    );

    console.log(`Webhook created for ${doctype} - ${event}`);
    return response.data;
  } catch (error) {
    console.error(`Error creating webhook for ${doctype} - ${event}:`, error.response?.data || error.message);
    return null;
  }
}

// Function to set up all webhooks
async function setupAllWebhooks() {
  console.log('Setting up ERPNext webhooks for blockchain integration...');

  const results = [];

  for (const doctype of DOCUMENT_TYPES) {
    for (const event of EVENTS) {
      const result = await createWebhook(doctype, event);
      if (result) {
        results.push({
          doctype,
          event,
          webhook_id: result.data.name,
          status: 'created'
        });
      }
    }
  }

  // Save results
  const resultsPath = path.resolve(__dirname, 'webhook-setup-results.json');
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2));

  console.log(`All webhooks setup completed. Results saved to ${resultsPath}`);
  console.log(`Total webhooks created: ${results.length}`);
}

// Run the webhook setup
setupAllWebhooks();