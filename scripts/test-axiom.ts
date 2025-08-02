import axios from 'axios';
import { config } from 'dotenv';

config();

const AXIOM_TOKEN = process.env.AXIOM_TOKEN;
const AXIOM_DATASET = process.env.AXIOM_DATASET || 'nft-bidding-bot';
const AXIOM_URL = 'https://api.axiom.co/v1/datasets';

async function testAxiomConnection() {
  console.log('Testing Axiom connection...');
  console.log('Token:', AXIOM_TOKEN ? `${AXIOM_TOKEN.substring(0, 10)}...` : 'NOT SET');
  console.log('Dataset:', AXIOM_DATASET);

  if (!AXIOM_TOKEN) {
    console.error('AXIOM_TOKEN not found in environment variables');
    return;
  }

  try {
    // Test with a single log entry
    const testLog = {
      _time: new Date().toISOString(),
      level: 'INFO',
      message: 'Test log from NFT bidding bot',
      category: 'test',
      username: process.env.USERNAME || 'UNKNOWN',
      server_type: 'nft_bidding_bot',
      test: true
    };

    console.log('Sending test log:', testLog);

    const response = await axios.post(
      `${AXIOM_URL}/${AXIOM_DATASET}/ingest`,
      [testLog], // Axiom expects an array of logs
      {
        headers: {
          'Authorization': `Bearer ${AXIOM_TOKEN}`,
          'Content-Type': 'application/json'
        },
        timeout: 10000
      }
    );

    console.log('✅ Success! Response:', {
      status: response.status,
      statusText: response.statusText,
      data: response.data
    });
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error('❌ Failed to send test log to Axiom:', {
        message: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        headers: error.response?.headers
      });
    } else {
      console.error('❌ Error:', error);
    }
  }
}

testAxiomConnection();