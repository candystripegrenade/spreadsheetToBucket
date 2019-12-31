// DEPS
const { Storage } require('@google-cloud/storage');
const { GoogleAuth } require('google-auth-library');
const { google } require('googleapis');
const path = require('path');
const { Transform } = require('json2csv');
const {
  writeFile,
  createReadStream,
  createWriteStream
}= require('fs').promises;

// ENV
const PROJECT_NAME = process.env('PROJECT_NAME');
const SHEET_ID = process.env('SHEET_ID');
const SHEET_TAB = process.env('SHEET_TAB');
const SHEET_COLS = process.env('SHEET_COLS');
const SHEET_RANGE = process.env('SHEET_RANGE');
const BUCKET_NAME = process.env('BUCKET_NAME');
const BUCKET_FILE_NAME = process.env('BUCKET_FILE_NAME');
const ENCODING = process.env('ENCODING');

/**
  @getBucketOpts: retrieves the bucket options for storage ops
  @param: { String } filePath
  @returns: { Object }
*/
function getBucketOpts(filePath) {
  return {
    destination: filePath,
    resumable: true,
    //kmsKeyName: getAdminKMSPath(),
    private: true,
    predefinedAcl: 'projectPrivate'
  };
}

/**
  @getCSVOpts: returns the necessary options for the csv parser
  @returns: { Object }
*/
function getCSVOpts() {
  return {
    fields: SHEET_COLS.split(',')
  };
}

/**
  @getTransformOpts: yields the options for the transformation
  @returns: { Object }
*/
function getTransformOpts() {
  return {
    encoding: ENCODING
  };
}

/**
  @getStreamOpts: yields the options to use for the streams
  @returns: { Object }
*/
function getStreamOpts() {
  return {
    gzip: true,
    encoding: ENCODING
  };
}

/**
  @getBucket: yields the bucket to store the csv in
  @returns: { Promise }
*/
async function getBucket() {
  const client = new Storage();
  const bucket = await client.bucket(BUCKET_NAME);

  return bucket;
}

/**
  @getSheet: retrieves the google sheet and parses it as JSON
  @returns: { Promise }
*/
async function getSheet() {
  const encoding = 'utf8';
  const version = 'v4';
  const sheets = google.sheets({ version });
  const spreadsheet = await sheets.spreadsheets.values.get({
    spreadsheetID: SHEET_ID,
    range: `${SHEET_TAB}!${SHEET_RANGE}`,
  });

  console.log('what is the data type?', spreadsheet.data.values);
  const payload = JSON.stringify(spreadsheet.data.values);

  return payload;
}

/**
  @getAndUploadReport: abstraction of all the mini utilities of the operation
  @param: { Object } body
  @returns: { Promise }
*/
async function getAndUploadReport(fileName) {
  try {

    const tmpPath = './tmp.json';
    const writePath = `./${SHEET_TAB}.csv`;
    const csvOpts = getCSVOpts();
    const storageOpts = getBucketOpts(writePath);
    const transOpts = getTransformOpts();
    const streamOpts = getStreamOpts();
    const transform = new Transform(csvOpts, transOpts);
    const bucket = await getBucket();
    const sheet = await getSheet();
    const toFile = await fs.writeFile(tmpPath, payload);
    const reader = await createReadStream(tmpPath, streamOpts);
    const writer = await createWriteStream(writePath, streamOpts);
    const create = await reader.pipe(transform).pipe(writer);

    if (bucket.file(writePath).exists()) {
        const kill = await bucket.file(writePath).delete();
    }

    const upload = await bucket.upload(writePath, opts);

    return upload;

  } catch(e) {
    return `getAndUploadReportError: ${e}`;
  }
}

/**
  @getReport: retrieves the report, then dumps it into the respective bucket
  @param: { Object } req
  @param: { Object } res
  @returns: { Response }
*/
exports.uploadSheetToBucket = (req, res) => {
  getAndUploadReport(req.body)
  .then(() => res.status(200).send('Uploaded Report'))
  .catch(e => res.status(500).send(`Error: ${e}`));
};
