// DEPS
const { Storage } = require('@google-cloud/storage');
const { auth } = require('google-auth-library');
const { google } = require('googleapis');
const { convertArrayToCSV } = require('convert-array-to-csv');
const { writeFile } = require('fs').promises;
const { tmpdir } = require('os');

// ENV
const {
  PROJECT_NAME,
  SHEET_ID,
  SHEET_TAB,
  SHEET_COLS,
  SHEET_RANGE,
  BUCKET_NAME,
  ENCODING
} = process.env;

/**
  Retrieves the bucket options for storage ops
  @param: { String } filename
  @returns: { Object }
*/
function getBucketOpts(filename) {
  return {
    destination: filename,
    resumable: false,
    private: true,
    predefinedAcl: 'projectPrivate'
  };
}

/**
  Parses the environment, then yields the storage client options
  @returns: { Object }
*/
function getStorageClientOpts() {
  const {
    client_email,
    private_key
  } = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);

  return {
    projectId: PROJECT_NAME,
    credentials: {
      client_email,
      private_key,
    }
  };
}

/**
  Yields the bucket to store the csv in
  @returns: { Promise }
*/
async function getBucket() {
  const storageOpts = getStorageClientOpts();
  const client = new Storage(storageOpts);
  const bucket = await client.bucket(BUCKET_NAME);

  return bucket;
}

/**
  Retrieves the authentication components for the sheets api
  @returns: { Object }
*/
function getSheetsAuth() {
  const credentials = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  const client = auth.fromJSON(credentials);

  client.scopes = 'https://www.googleapis.com/auth/spreadsheets.readonly';

  return client;
}

/**
  Retrieves the google sheet and parses it as JSON
  @returns: { Promise }
*/
async function getSheet() {
  try {
    const version = 'v4';
    const header = SHEET_COLS.split(',');
    const auth = getSheetsAuth();
    const sheets = google.sheets({ version, auth });
    const spreadsheet = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_TAB}!${SHEET_RANGE}`,
    });

    return convertArrayToCSV(spreadsheet.data.values, {
      header,
      separator: ','
    });

  } catch(e) {
    throw new Error(`getSheet(): ${e}`);
  }
}

/**
  Abstraction of all the mini utilities of the operation
  @param: { Object } body
  @returns: { Promise }
*/
async function getAndUploadReport(body) {
  try {
    const filename = `${SHEET_TAB}.csv`;
    const writePath = `${tmpdir()}/${filename}`;
    const storageOpts = getBucketOpts(filename);
    const bucket = await getBucket();
    const csv = await getSheet();
    const f = await writeFile(writePath, csv);
    const exists = await bucket.file(filename).exists();

    // kinda weird that the exists promise returns an array of size 1
    if (exists[0]) {
        const kill = await bucket.file(filename).delete();
    }

    const upload = await bucket.upload(writePath, storageOpts);

    return upload;

  } catch(e) {
    throw new Error(`getAndUploadReportError: ${e}`);
  }
}

/**
  Retrieves the report, then dumps it into the respective bucket
  @param: { Object } req
  @param: { Object } res
  @returns: { Response }
*/
exports.uploadSheetToBucket = (req, res) => {
  getAndUploadReport(req.body)
  .then(() => res.status(200).send('Uploaded Report'))
  .catch(e => res.status(500).send(`Error: ${e}`));
};
