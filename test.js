const { handler } = require('./index'); // Replace './index' with your Lambda file name
const fs = require('fs');

(async () => {
  try {
    // Load the test event
    const event = JSON.parse(fs.readFileSync('./testEvent.json', 'utf8'));

    // Invoke the handler function
    const result = await handler(event);
    console.log('Lambda function executed successfully:', result);
  } catch (error) {
    console.error('Error while executing the Lambda function:', error);
  }
})();
