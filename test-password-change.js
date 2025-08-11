// Test script for password change endpoint
// This demonstrates how to use the new /api/auth/change-password endpoint

const crypto = require('crypto');

// Helper function to encrypt password (same as client would use)
function encryptPassword(password) {
  const publicKey = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3K7hm5//T+CLHQ1xN8b7
ISPRwFT3hQFcpNe1j9CkjbEJxfH3269Y3bY1emg+XVzaDhSVpJyiJ7L2HxUmO7Hs
VCp4L0m0S9K5v7IIuL0gMJh2F6PLkSPwrjcxGIhMMMu0jI0w9w7EqWaShV3ktiPD
Wd/fQWT3qWQJqE0Tf9x2BzM9k6zKqBQP/7PaLI3UqBXQE2YYm+qC5sy3qXc7rQWi
HKWLt7TbFAV1KSPPrFw0EJLPKNbE7KPlbUJiVfZxaKlw5ITyKGqrzF5uFqh5Tq8I
dHzJJAX0xSUMeLCrJKGzHH7Ds0r3GOgY6vhLKLqThlWUEq0KrPnkTBCNDm5KQUCI
fQIDAQAB
-----END PUBLIC KEY-----`;
  
  const encrypted = crypto.publicEncrypt(
    {
      key: publicKey,
      padding: crypto.constants.RSA_PKCS1_OAEP_PADDING,
      oaepHash: 'sha256'
    },
    Buffer.from(password)
  );
  
  return encrypted.toString('base64');
}

// Example usage
async function testPasswordChange() {
  const oldPassword = 'your-current-password';
  const newPassword = 'your-new-password';
  
  const requestBody = {
    encryptedOldPassword: encryptPassword(oldPassword),
    encryptedNewPassword: encryptPassword(newPassword)
  };
  
  console.log('Request body:', JSON.stringify(requestBody, null, 2));
  
  // Make the request to your server
  // Example with fetch (Node.js 18+) or use axios
  /*
  const response = await fetch('http://localhost:3003/api/auth/change-password', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody)
  });
  
  const result = await response.json();
  console.log('Response:', result);
  */
}

// testPasswordChange();

console.log(`
To use the password change endpoint:

1. Call POST /api/auth/change-password with:
   {
     "encryptedOldPassword": "base64-encrypted-old-password",
     "encryptedNewPassword": "base64-encrypted-new-password"
   }

2. The endpoint will:
   - Verify the old password matches the current password
   - Re-encrypt all wallet private keys with the new password
   - Update the password in memory/Redis
   - Return success or error with details

3. If some wallets fail to re-encrypt, the password won't be changed and 
   you'll get a list of failed wallets to investigate.
`);