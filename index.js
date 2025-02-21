const AWS = require('aws-sdk');
const { Client } = require('pg'); // PostgreSQL client
const { simpleParser } = require('mailparser'); // For parsing emails
const axios = require('axios'); // For making requests to OpenAI
const logger = console;

// Environment variables
const DB_CONFIG = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: "hubduck",
  port: 5432,
  ssl: { rejectUnauthorized: false },
};

const s3 = new AWS.S3({ region: process.env.AWS_REGION });
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
logger.info(`OpenAI API Key: ${OPENAI_API_KEY ? 'Set' : 'Not Set'}`);
const OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"; // ChatGPT endpoint

// Function to establish a database connection
async function getDbConnection() {
  logger.info('Connecting to database...');
  const client = new Client(DB_CONFIG);
  await client.connect();
  logger.info('Database connection established.');
  return client;
}

// Fetch and parse email from S3
async function parseEmailFromS3(bucket, key) {
  logger.info(`Fetching email from S3: Bucket: ${bucket}, Key: ${key}`);
  const response = await s3.getObject({ Bucket: bucket, Key: key }).promise();
  const emailContent = response.Body.toString('utf-8');
  const parsedEmail = await simpleParser(emailContent);
  return {
    subject: parsedEmail.subject || '(No Subject)',
    from: parsedEmail.from?.text || '(Unknown Sender)',
    to: parsedEmail.to?.text || '(Unknown Recipient)',
    date: parsedEmail.date || new Date(),
    message_id: parsedEmail.messageId || '',
    content: parsedEmail.text || parsedEmail.html || '(No Content)',
  };
}

// Fetch duck ID from the database
async function getDuckId(client, toAddress) {
  logger.info(`Looking up duck for address: ${toAddress}`);
  const [emailPrefix, orgSlug] = toAddress.split('@')[0].split('.');
  logger.info(`Email prefix: ${emailPrefix}, Org slug: ${orgSlug}`);

  const query = `
    SELECT d.id 
    FROM ducks d
    JOIN "Organization" o ON d.org_id = o.id
    WHERE d.email_prefix = $1
    AND o.slug = $2
    AND d.is_active = true
  `;

  const result = await client.query(query, [emailPrefix, orgSlug]);

  if (result.rows.length === 0) {
    throw new Error(`No active duck found for ${toAddress}`);
  }

  return result.rows[0].id;
}

// Fetch prompts for the duck type
async function getPromptsForDuckType(client, duckId) {
  logger.info(`Fetching prompts for duck ID: ${duckId}`);

  // Validate UUID
  const isValidUUID = (id) =>
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id);
  if (!isValidUUID(duckId)) {
    throw new Error(`Invalid UUID: ${duckId}`);
  }

  // Step 1: Check for duck-specific overrides first
  const overrideQuery = `
    SELECT po.prompt_text
    FROM duck_prompt_overrides po
    JOIN prompt_templates pt ON po.template_id = pt.id
    WHERE po.duck_id = $1
    AND po.is_active = true
  `;
  
  const overrideResult = await client.query(overrideQuery, [duckId]);
  
  if (overrideResult.rows.length > 0) {
    logger.info('Using duck-specific prompt overrides');
    return overrideResult.rows.map(row => ({
      promptText: row.prompt_text
    }));
  }

  // Step 2: If no overrides, get the duck's type and use type-specific prompts
  const duckQuery = `
    SELECT type
    FROM ducks
    WHERE id = $1::uuid
  `;
  const duckResult = await client.query(duckQuery, [duckId]);
  if (duckResult.rows.length === 0) {
    throw new Error(`Duck not found for ID: ${duckId}`);
  }
  const duckType = duckResult.rows[0].type;

  // Step 3: Get the latest active prompts for this duck type
  const typePromptsQuery = `
    SELECT dtp.prompt_text
    FROM duck_type_prompts dtp
    JOIN duck_types dt ON dtp.duck_type_id = dt.id
    WHERE dt.name = $1
    AND dtp.is_active = true
    ORDER BY dtp.version DESC
  `;
  
  const typePromptsResult = await client.query(typePromptsQuery, [duckType]);

  if (typePromptsResult.rows.length === 0) {
    logger.warn(`No type-specific prompts found for type ${duckType}, using default prompts`);
    return [{
      promptText: `Process this email focusing on key information including:
1. Events and dates
2. Action items and deadlines
3. Important updates or changes
4. Required responses or decisions`
    }];
  }

  return typePromptsResult.rows.map(row => ({
    promptText: row.prompt_text
  }));
}

async function processEmailWithAI(combinedPrompt) {
  logger.info('Processing email with OpenAI...');
  try {
    const response = await axios.post(
      OPENAI_API_URL,
      {
        model: "gpt-4",
        messages: [
          { 
            role: "system", 
            content: "You are an AI assistant that processes emails and extracts structured information." 
          },
          { 
            role: "user", 
            content: combinedPrompt 
          }
        ],
        temperature: 0.3, // Lower temperature for more consistent outputs
        max_tokens: 1000
      },
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
      }
    );

    const result = response.data.choices[0].message.content;
    logger.info('Successfully processed email with AI');
    return result;
  } catch (error) {
    logger.error('Error processing email with AI:', error.message);
    throw new Error(`OpenAI processing failed: ${error.message}`);
  }
}

function combinePrompts(emailContent, prompts) {
  // Combine all prompt texts from the prompts array
  const promptInstructions = prompts.map(p => p.promptText).join('\n\n');
  
  let combinedPrompt = `${promptInstructions}\n\nAnalyze the following email:\n\n${emailContent}\n\n`;

  // Add default structure if no specific format is provided in the prompts
  if (!promptInstructions.includes('JSON format')) {
    combinedPrompt += `Respond in the following JSON format:\n{
  "events": [
    {
      "title": "Event title",
      "date": "YYYY-MM-DD",
      "time": "HH:MM AM/PM",
      "location": "Event location",
      "description": "Detailed description of the event"
    }
  ],
  "actions": [
    {
      "type": "ACTION_TYPE",
      "description": "Action description",
      "deadline": "YYYY-MM-DD",
      "priority": "HIGH|MEDIUM|LOW"
    }
  ],
  "summary": "Brief summary of the email",
  "categories": ["category1", "category2"],
  "importance": "HIGH|MEDIUM|LOW"
}`;
  }

  return combinedPrompt;
}

// Process email with OpenAI API
async function extractAndSaveEmailActions(client, emailId, duckId, aiResults) {
  logger.info(`Extracting and saving email actions for email ID: ${emailId}`);

  try {
    const parsedResults = JSON.parse(aiResults);

    // Check if events are present in AI results
    if (parsedResults.events && Array.isArray(parsedResults.events) && parsedResults.events.length > 0) {
      for (const event of parsedResults.events) {
        const { title, date, time, location, description } = event;

        // Create a meaningful action note
        const note = title || description || "No title provided";

        // Check if the action already exists
        const existingActionQuery = `
          SELECT id 
          FROM email_actions 
          WHERE user_id = $1 AND email_id = $2 AND action_type = $3
        `;
        const userId = await getDefaultUserId(client, duckId); // Ensure user ID is retrieved correctly
        const existingAction = await client.query(existingActionQuery, [userId, emailId, 'EVENT']);

        if (existingAction.rows.length > 0) {
          logger.warn(`Duplicate action detected for email ID: ${emailId}, skipping insertion.`);
          continue; // Skip if a duplicate exists
        }

        // Insert event into email_actions table
        await client.query(
          `
          INSERT INTO email_actions (
            email_id, 
            duck_id, 
            user_id, 
            action_type, 
            context, 
            note, 
            status, 
            created_at, 
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [
            emailId,
            duckId,
            userId,
            'EVENT', // Action type
            JSON.stringify({ date, time, location }), // Context
            note, // Note
            'ACTIVE', // Status
          ]
        );

        logger.info(`Saved event action for email ID: ${emailId}`);
      }
    } else {
      logger.info(`No events found in AI results for email ID: ${emailId}`);
    }
  } catch (error) {
    logger.error(`Failed to extract and save email actions for email ID: ${emailId}`, error);
    throw error;
  }
}

// Helper function to fetch a default user ID
async function getDefaultUserId(client, duckId) {
  try {
    const query = `
      SELECT id 
      FROM users
      WHERE organization_id = (
        SELECT org_id 
        FROM ducks 
        WHERE id = $1
      )
      LIMIT 1
    `;
    const result = await client.query(query, [duckId]);

    if (result.rows.length > 0) {
      return result.rows[0].id;
    }

    // Fallback: Assign a default user ID if no user is found
    logger.warn(`No users found for duck ID: ${duckId}. Assigning default user.`);
    const defaultUserId = await getDefaultAdminUserId(client);
    return defaultUserId;
  } catch (error) {
    logger.error(`Error fetching default user ID for duck ID: ${duckId}`, error);
    throw error;
  }
}

// Fallback: Fetch a global default admin user ID
async function getDefaultAdminUserId(client) {
  const query = `
    SELECT id 
    FROM users
    WHERE role = 'SCHOOLADMIN'
    LIMIT 1
  `;
  const result = await client.query(query);

  if (result.rows.length === 0) {
    throw new Error('No default admin user found. Please ensure a default admin exists in the database.');
  }

  return result.rows[0].id;
}

// Save the parsed email to the database
async function saveEmailToDb(client, duckId, parsedEmail, s3Key) {
  logger.info('Saving email to the database...');
  const query = `
    INSERT INTO processed_emails 
    (duck_id, message_id, subject, sender, received_at, s3_key, 
    processing_status, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    RETURNING id
  `;
  const values = [
    duckId,
    parsedEmail.message_id,
    parsedEmail.subject,
    parsedEmail.from,
    parsedEmail.date,
    s3Key,
    'RECEIVED',
  ];
  const result = await client.query(query, values);
  return result.rows[0].id;
}

async function testOpenAIConnection() {
  try {
    const response = await axios.post(
      OPENAI_API_URL,
      {
        model: "gpt-4",
        messages: [{ role: "system", content: "Connection Test" }],
      },
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "Content-Type": "application/json",
        },
        timeout: 5000, // Set timeout to 5 seconds for a quick check
      }
    );
    logger.info('Test OpenAI API Response:', JSON.stringify(response.data));
    return true;
  } catch (error) {
    logger.error('OpenAI Connection Test Failed:', error.message);
    if (error.response) {
      logger.error('Response Status:', error.response.status);
      logger.error('Response Data:', JSON.stringify(error.response.data));
    }
    return false;
  }
}

// Save AI-processed results to the database
async function saveProcessedResults(client, emailId, aiResults) {
  logger.info(`Saving AI results for email ID: ${emailId}`);
  try {
    // Validate JSON
    const parsedResults = JSON.parse(aiResults);

    const query = `
      UPDATE processed_emails
      SET processing_status = 'PROCESSED', extracted_data = $1, updated_at = CURRENT_TIMESTAMP
      WHERE id = $2
    `;
    await client.query(query, [parsedResults, emailId]);
    logger.info(`Successfully updated email ID: ${emailId} with AI results`);
  } catch (error) {
    logger.error(`Invalid AI results JSON for email ID: ${emailId}`, { aiResults });
    throw new Error(`Failed to save AI results: ${error.message}`);
  }
}

async function testInternetConnection() {
  try {
    const response = await axios.get('https://www.google.com', { timeout: 5000 });
    logger.info('Internet Connection Test Successful:', response.status);
    return true;
  } catch (error) {
    logger.error('Internet Connection Test Failed:', error.message);
    return false;
  }
}

// Save email actions to the email_actions table
async function saveEmailActions(client, emailId, duckId, aiResults) {
  logger.info(`Extracting and saving email actions for email ID: ${emailId}`);

  try {
    const parsedResults = JSON.parse(aiResults);

    // Check for events in the parsed AI results
    if (parsedResults.events && parsedResults.events.length > 0) {
      for (const event of parsedResults.events) {
        const { date, description, location } = event;

        // Create a meaningful action note
        const note = `Event scheduled on ${date || 'unknown date'} at ${location || 'unknown location'}: ${description}`;

        // Insert the action into the email_actions table
        await client.query(
          `
          INSERT INTO email_actions (email_id, duck_id, action_type, context, note, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [
            emailId,
            duckId,
            'EVENT',
            JSON.stringify(event), // Store the event context in JSON format
            note,
          ]
        );

        logger.info(`Saved email action: ${note}`);
      }
    } else {
      logger.info(`No events found in AI results for email ID: ${emailId}`);
    }
  } catch (error) {
    logger.error(`Failed to save email actions for email ID: ${emailId}`, error);
    throw new Error(`Failed to save email actions: ${error.message}`);
  }
}

// Main Lambda handler
exports.handler = async (event) => {
  logger.info('Starting email processing...');
  let client;

  try {
    client = await getDbConnection();

    for (const record of event.Records) {
      const bucket = record.s3.bucket.name;
      const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

      try {
        const internetStatus = await testInternetConnection();
if (!internetStatus) {
  throw new Error('Internet access not available from Lambda. Check VPC and NAT Gateway configuration.');
}

        const connectionStatus = await testOpenAIConnection();
        if (!connectionStatus) {
          throw new Error('OpenAI API connection test failed. Check your API key and network settings.');
        }

        // Step 1: Parse email from S3
        const parsedEmail = await parseEmailFromS3(bucket, key);

        // Step 2: Get duck ID
        const duckId = await getDuckId(client, parsedEmail.to);

        // Step 3: Save email to database
        const emailId = await saveEmailToDb(client, duckId, parsedEmail, key);

        // Step 4: Fetch and combine prompts
        const prompts = await getPromptsForDuckType(client, duckId);
        const combinedPrompt = combinePrompts(parsedEmail.content, prompts);

        // Step 5: Process email with OpenAI
        const aiResults = await processEmailWithAI(combinedPrompt);

 // Step 6: Save AI results to database
 await saveProcessedResults(client, emailId, aiResults);

 // Step 7: Extract and save email actions
 await extractAndSaveEmailActions(client, emailId, duckId, aiResults);

        logger.info(`Successfully processed email with ID: ${emailId}`);
      } catch (innerError) {
        logger.error(`Error processing record ${JSON.stringify(record)}: ${innerError.message}`);
      }
    }

    return { statusCode: 200, body: JSON.stringify({ message: 'Successfully processed all emails' }) };
  } catch (err) {
    logger.error(`Error in Lambda handler: ${err.message}`);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  } finally {
    if (client) {
      await client.end();
      logger.info('Database connection closed.');
    }
  }
};

