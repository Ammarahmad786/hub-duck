const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Client } = require('pg'); // Use 'pg' module for PostgreSQL connections
const dotenv = require('dotenv');
dotenv.config();

// Set up logging
const logger = console;

// Database connection settings from environment variables
const DB_CONFIG = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: 'hubduck',
  port: 5432,
  ssl: { rejectUnauthorized: false }, // Allow SSL
};

// Create an S3 Client
const s3Client = new S3Client({ region: process.env.AWS_REGION });

// Function to get a database connection
const getDbConnection = async () => {
  logger.info('Creating database connection...');
  const client = new Client(DB_CONFIG);
  try {
    await client.connect();
    return client;
  } catch (error) {
    logger.error(`Database connection failed: ${error.message}`);
    throw error;
  }
};

// Function to parse email from S3
const parseEmailFromS3 = async (bucket, key) => {
  logger.info(`Getting email from S3: ${bucket}/${key}`);
  try {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(command);
    const emailContent = await streamToString(response.Body);

    // Example of simple parsing (a proper email parser like 'mailparser' can be used for production)
    const parsedEmail = {
      subject: emailContent.match(/Subject: (.+)/)?.[1] || '',
      from: emailContent.match(/From: (.+)/)?.[1] || '',
      to: emailContent.match(/To: (.+)/)?.[1] || '',
      date: emailContent.match(/Date: (.+)/)?.[1] || '',
      content: emailContent, // Simplified, stores the full content
    };

    logger.info(`Successfully parsed email: ${parsedEmail.subject}`);
    return parsedEmail;
  } catch (error) {
    logger.error(`Error parsing email from S3: ${error.message}`);
    throw error;
  }
};
// const parseEmailFromS3 = async (bucket, key) => {
//     logger.info(`Mocking S3 email fetch: ${bucket}/${key}`);
//     return {
//       subject: "Test Email Subject",
//       from: "test@example.com",
//       to: "test.duck@hubduck.net",
//       date: new Date().toISOString(),
//       content: "This is a test email content",
//     };
//   };
  
// Helper function to convert S3 stream to string
const streamToString = (stream) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (chunk) => chunks.push(chunk));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
  });

// Function to get duck_id based on email address
const getDuckId = async (client, toAddress) => {
  logger.info(`Looking up duck for address: ${toAddress}`);
  const [emailPrefix, orgSlug] = toAddress.split('@')[0].split('.');

  try {
    const query = `
      SELECT d.id 
      FROM ducks d
      JOIN organizations o ON d.org_id = o.id
      WHERE d.email_prefix = $1
      AND o.slug = $2
      AND d.is_active = true
    `;
    const values = [emailPrefix, orgSlug];
    const result = await client.query(query, values);

    if (result.rows.length === 0) {
      throw new Error(`No active duck found for ${toAddress}`);
    }

    const duckId = result.rows[0].id;
    logger.info(`Found duck_id: ${duckId}`);
    return duckId;
  } catch (error) {
    logger.error(`Error getting duck_id: ${error.message}`);
    throw error;
  }
};

// Function to save email to the database
const saveEmailToDb = async (client, duckId, parsedEmail, s3Key) => {
  logger.info('Saving email to database...');
  const query = `
    INSERT INTO processed_emails 
    (duck_id, message_id, subject, sender, received_at, s3_key, 
    processing_status, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    RETURNING id
  `;
  const values = [
    duckId,
    parsedEmail.message_id || null,
    parsedEmail.subject || null,
    parsedEmail.from || null,
    parsedEmail.date || null,
    s3Key,
    'RECEIVED',
  ];

  try {
    const result = await client.query(query, values);
    const emailId = result.rows[0].id;
    logger.info(`Successfully saved email with ID: ${emailId}`);
    return emailId;
  } catch (error) {
    logger.error(`Error saving email to database: ${error.message}`);
    throw error;
  }
};

// Main Lambda handler
exports.handler = async (event) => {
  logger.info('Starting email processing...');
  let client;

  try {
    // Connect to the database
    client = await getDbConnection();

    // Process each record
    for (const record of event.Records) {
      const body = JSON.parse(record.body);
      const s3Event = body.Records[0].s3;
      const bucket = s3Event.bucket.name;
      const key = s3Event.object.key;

      // Parse the email
      const parsedEmail = await parseEmailFromS3(bucket, key);

      // Get duck_id
      const duckId = await getDuckId(client, parsedEmail.to);

      // Save email to the database
      const emailId = await saveEmailToDb(client, duckId, parsedEmail, key);

      logger.info(`Successfully processed email: ${emailId}`);
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Successfully processed all emails' }),
    };
  } catch (error) {
    logger.error(`Processing failed: ${error.message}`);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  } finally {
    if (client) {
      await client.end();
      logger.info('Database connection closed');
    }
  }
};
